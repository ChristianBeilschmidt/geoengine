use crate::engine::{
    InitializedOperator, InitializedOperatorImpl, InitializedRasterOperator, Operator,
    QueryContext, QueryProcessor, QueryRectangle, RasterOperator, RasterQueryProcessor,
    RasterResultDescriptor, TypedRasterQueryProcessor,
};
use crate::opencl::cl_program::{CLProgram, CompiledCLProgram, IterationType, RasterArgument};
use crate::util::Result;
use futures::stream::BoxStream;
use futures::StreamExt;
use geoengine_datatypes::projection::ProjectionOption;
use geoengine_datatypes::raster::{Pixel, Raster, Raster2D, RasterDataType, RasterTile2D};
use serde::{Deserialize, Serialize};
use snafu::ensure;
use std::collections::HashSet;
use std::convert::TryFrom;
use std::marker::PhantomData;

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
pub struct ExpressionParams {
    pub expression: String,
    pub output_type: RasterDataType,
}

// TODO: custom type or simple string?
struct SafeExpression {
    expression: String,
}

pub type Expression = Operator<ExpressionParams>;

impl Expression {
    // TODO: also check this when creating original operator parameters
    fn is_allowed_expression(expression: &str) -> bool {
        // TODO: perform actual syntax checking
        let disallowed_chars: HashSet<_> = ";[]{}#%\"\'\\".chars().collect();
        let disallowed_strs: HashSet<String> =
            vec!["/*".into(), "//".into(), "*/".into(), "return".into()]
                .into_iter()
                .collect();

        expression.chars().all(|c| !disallowed_chars.contains(&c))
            && disallowed_strs.iter().all(|s| !expression.contains(s))
    }
}

#[typetag::serde]
impl RasterOperator for Expression {
    fn initialize(
        self: Box<Self>,
        context: crate::engine::ExecutionContext,
    ) -> Result<Box<InitializedRasterOperator>> {
        ensure!(
            Self::is_allowed_expression(&self.params.expression),
            crate::error::InvalidExpression
        );

        InitializedOperatorImpl::create(
            self.params,
            context,
            |_, _, _, _| Ok(()),
            |params, _, _, _, _| {
                Ok(RasterResultDescriptor {
                    data_type: params.output_type,
                    projection: ProjectionOption::None, // TODO
                })
            },
            self.raster_sources,
            vec![], // TODO: is it ok to omit them?
        )
        .map(InitializedOperatorImpl::boxed)
    }
}

impl InitializedOperator<RasterResultDescriptor, TypedRasterQueryProcessor>
    for InitializedOperatorImpl<ExpressionParams, RasterResultDescriptor, ()>
{
    fn query_processor(&self) -> Result<TypedRasterQueryProcessor> {
        // TODO: match input variant combination
        // TODO: handle different number of sources

        match *self.raster_sources.as_slice() {
            [ref a, ref b] => {
                let a = a.query_processor()?;
                let b = b.query_processor()?;

                match (a, b) {
                    (TypedRasterQueryProcessor::I8(p_a), TypedRasterQueryProcessor::I8(p_b)) => {
                        Ok(match self.params.output_type {
                            RasterDataType::I8 => TypedRasterQueryProcessor::I8(
                                ExpressionQueryProcessor::new(
                                    &SafeExpression {
                                        expression: self.params.expression.clone(),
                                    },
                                    p_a,
                                    p_b,
                                )
                                .boxed(),
                            ),
                            _ => unimplemented!(),
                        })
                    }
                    _ => unimplemented!(),
                }
            }
            _ => unimplemented!(),
        }
    }
}

struct ExpressionQueryProcessor<T1, T2, TO>
where
    T1: Pixel,
    T2: Pixel,
    TO: Pixel,
{
    pub source_a: Box<dyn RasterQueryProcessor<RasterType = T1>>,
    pub source_b: Box<dyn RasterQueryProcessor<RasterType = T2>>,
    pub phantom_data: PhantomData<TO>,
    pub cl_program: CompiledCLProgram,
}

impl<T1, T2, TO> ExpressionQueryProcessor<T1, T2, TO>
where
    T1: Pixel,
    T2: Pixel,
    TO: Pixel,
{
    fn new(
        expression: &SafeExpression,
        source_a: Box<dyn RasterQueryProcessor<RasterType = T1>>,
        source_b: Box<dyn RasterQueryProcessor<RasterType = T2>>,
    ) -> Self {
        Self {
            source_a,
            source_b,
            cl_program: Self::create_cl_program(&expression),
            phantom_data: PhantomData::default(),
        }
    }

    fn create_cl_program(expression: &SafeExpression) -> CompiledCLProgram {
        // TODO: generate code for arbitrary amount of inputs
        let source = r#"
__kernel void expressionkernel(
            __global const IN_TYPE0 *in_data0,
            __global const RasterInfo *in_info0,
            __global const IN_TYPE1* in_data1,
            __global const RasterInfo *in_info1,
            __global OUT_TYPE0* out_data,
            __global const RasterInfo *out_info)
{
    uint const gid = get_global_id(0) + get_global_id(1) * in_info0->size[0];
    if (gid >= in_info0->size[0]*in_info0->size[1]*in_info0->size[2])
        return;

    IN_TYPE0 A = in_data0[gid];
    if (ISNODATA0(A, in_info0)) {
        out_data[gid] = out_info->no_data;
        return;
    }

    IN_TYPE0 B = in_data1[gid];
    if (ISNODATA1(B, in_info1)) {
        out_data[gid] = out_info->no_data;
        return;
    }

    OUT_TYPE0 result = %%%EXPRESSION%%%;
	out_data[gid] = result;
}"#
        .replace("%%%EXPRESSION%%%", &expression.expression);

        let mut cl_program = CLProgram::new(IterationType::Raster);
        cl_program.add_input_raster(RasterArgument::new(T1::TYPE));
        cl_program.add_input_raster(RasterArgument::new(T2::TYPE));
        cl_program.add_output_raster(RasterArgument::new(TO::TYPE));

        cl_program.compile(&source, "expressionkernel").unwrap()
    }
}

impl<'a, T1, T2, TO> QueryProcessor for ExpressionQueryProcessor<T1, T2, TO>
where
    T1: Pixel,
    T2: Pixel,
    TO: Pixel,
{
    type Output = RasterTile2D<TO>;

    fn query(
        &self,
        query: QueryRectangle,
        ctx: QueryContext,
    ) -> BoxStream<Result<RasterTile2D<TO>>> {
        // TODO: remove if zip-map approach suffices
        // ExpressionStream {
        //     stream: self
        //         .source_a
        //         .query(query, ctx)
        //         .zip(self.source_b.query(query, ctx)),
        //     phantom: Default::default(),
        // }
        // .boxed()

        // TODO: validate that tiles actually fit together, otherwise we need a different approach
        let mut cl_program = self.cl_program.clone();
        self.source_a
            .query(query, ctx)
            .zip(self.source_b.query(query, ctx))
            .map(move |(a, b)| match (a, b) {
                (Ok(a), Ok(b)) => {
                    let mut out = Raster2D::new(
                        *a.dimension(),
                        vec![TO::zero(); a.data.data_container.len()], // TODO: correct output size; initialization required?
                        None,                                          // TODO
                        Default::default(),                            // TODO
                        Default::default(),                            // TODO
                    )
                    .expect("raster creation must succeed")
                    .into();

                    let a_typed = a.data.into();
                    let b_typed = b.data.into();
                    let mut params = cl_program.params();

                    params.set_input_raster(0, &a_typed).unwrap();
                    params.set_input_raster(1, &b_typed).unwrap();
                    params.set_output_raster(0, &mut out).unwrap();
                    cl_program.run(params).unwrap();

                    let raster = Raster2D::<TO>::try_from(out).expect("must be correct");

                    Ok(RasterTile2D::new(raster.temporal_bounds, a.tile, raster))
                }
                _ => unimplemented!(),
            })
            .boxed()
    }
}

// TODO: remove the following template if simple zip-map suffices for the operator

// #[pin_project(project = ExpressionStreamProjection)]
// pub struct ExpressionStream<St, T1, T2, TO>
// where
//     St: Stream<Item = (Result<RasterTile2D<T1>>, Result<RasterTile2D<T2>>)>,
//     T1: Pixel,
//     T2: Pixel,
//     TO: Pixel,
// {
//     #[pin]
//     stream: St,
//     phantom: PhantomData<TO>,
// }
//
// impl<St, T1, T2, TO> Stream for ExpressionStream<St, T1, T2, TO>
// where
//     St: Stream<Item = (Result<RasterTile2D<T1>>, Result<RasterTile2D<T2>>)>,
//     T1: Pixel,
//     T2: Pixel,
//     TO: Pixel,
// {
//     type Item = Result<RasterTile2D<TO>>;
//
//     fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
//         let ExpressionStreamProjection {
//             mut stream,
//             phantom,
//         } = self.as_mut().project();
//
//         let next = ready!(stream.as_mut().poll_next(cx));
//
//         if let Some(raster) = next {
//             // TODO: actually calculate the expression
//             Poll::Ready(Some(raster.0))
//         } else {
//             Poll::Ready(None)
//         }
//     }
// }

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::ExecutionContext;
    use crate::mock::{MockRasterSource, MockRasterSourceParams};
    use geoengine_datatypes::primitives::{BoundingBox2D, TimeInterval};
    use geoengine_datatypes::projection::Projection;
    use geoengine_datatypes::raster::TileInformation;

    #[tokio::test]
    async fn basic() {
        let a = make_raster();
        let b = make_raster();

        let o = Expression {
            params: ExpressionParams {
                expression: "A+B".to_string(),
                output_type: RasterDataType::I8,
            },
            raster_sources: vec![a, b],
            vector_sources: vec![],
        }
        .boxed()
        .initialize(ExecutionContext {})
        .unwrap();

        let p = o.query_processor().unwrap().get_i8().unwrap();

        let q = p.query(
            QueryRectangle {
                bbox: BoundingBox2D::new_unchecked((1., 2.).into(), (3., 4.).into()),
                time_interval: Default::default(),
            },
            QueryContext { chunk_byte_size: 1 },
        );

        let c: Vec<Result<RasterTile2D<i8>>> = q.collect().await;

        assert_eq!(c.len(), 1);

        assert_eq!(
            c[0].as_ref().unwrap().data,
            Raster2D::new(
                [3, 2].into(),
                vec![2, 4, 6, 8, 10, 12],
                None,
                Default::default(),
                Default::default(),
            )
            .unwrap()
        );
    }

    fn make_raster() -> Box<dyn RasterOperator> {
        let raster = Raster2D::new(
            [3, 2].into(),
            vec![1, 2, 3, 4, 5, 6],
            None,
            Default::default(),
            Default::default(),
        )
        .unwrap();

        let raster_tile = RasterTile2D {
            time: TimeInterval::default(),
            tile: TileInformation {
                geo_transform: Default::default(),
                global_pixel_position: [0, 0].into(),
                global_size_in_tiles: [1, 2].into(),
                global_tile_position: [0, 0].into(),
                tile_size_in_pixels: [3, 2].into(),
            },
            data: raster,
        };

        MockRasterSource {
            params: MockRasterSourceParams {
                data: vec![raster_tile],
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::I8,
                    projection: Projection::wgs84().into(),
                },
            },
        }
        .boxed()
    }
}
