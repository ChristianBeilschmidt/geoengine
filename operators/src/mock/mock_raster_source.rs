use crate::call_generic_raster_processor;
use crate::engine::{
    InitializedRasterOperator, RasterOperator, RasterQueryProcessor, RasterResultDescriptor,
    SourceOperator, TypedRasterQueryProcessor,
};
use crate::util::Result;
use async_trait::async_trait;
use futures::{stream, stream::StreamExt};
use geoengine_datatypes::primitives::SpatialPartitioned;
use geoengine_datatypes::raster::{FromPrimitive, Pixel, RasterTile2D};
use num_traits::AsPrimitive;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone)]
pub struct MockRasterSourceProcessor<T>
where
    T: Pixel,
{
    pub data: Vec<RasterTile2D<T>>,
}

impl<T> MockRasterSourceProcessor<T>
where
    T: Pixel,
{
    fn new(data: Vec<RasterTile2D<T>>) -> Self {
        Self { data }
    }
}

#[async_trait]
impl<T> RasterQueryProcessor for MockRasterSourceProcessor<T>
where
    T: Pixel,
{
    type RasterType = T;
    async fn raster_query<'a>(
        &'a self,
        query: crate::engine::RasterQueryRectangle,
        _ctx: &'a dyn crate::engine::QueryContext,
    ) -> Result<futures::stream::BoxStream<crate::util::Result<RasterTile2D<Self::RasterType>>>>
    {
        Ok(stream::iter(
            self.data
                .iter()
                .filter(move |t| {
                    t.time.intersects(&query.time_interval)
                        && t.tile_information()
                            .spatial_partition()
                            .intersects(&query.spatial_bounds)
                })
                .cloned()
                .map(Result::Ok),
        )
        .boxed())
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
#[serde(rename_all = "camelCase")]
pub struct MockRasterSourceParams {
    pub data: Vec<RasterTile2D<u8>>,
    pub result_descriptor: RasterResultDescriptor,
}

pub type MockRasterSource = SourceOperator<MockRasterSourceParams>;

#[typetag::serde]
#[async_trait]
impl RasterOperator for MockRasterSource {
    async fn initialize(
        self: Box<Self>,
        _context: &dyn crate::engine::ExecutionContext,
    ) -> Result<Box<dyn InitializedRasterOperator>> {
        Ok(InitializedMockRasterSource {
            result_descriptor: self.params.result_descriptor,
            data: self.params.data,
        }
        .boxed())
    }
}

pub struct InitializedMockRasterSource {
    result_descriptor: RasterResultDescriptor,
    data: Vec<RasterTile2D<u8>>,
}

impl InitializedRasterOperator for InitializedMockRasterSource {
    fn query_processor(&self) -> Result<TypedRasterQueryProcessor> {
        fn converted<From, To>(
            raster_tiles: &[RasterTile2D<From>],
        ) -> Box<dyn RasterQueryProcessor<RasterType = To>>
        where
            From: Pixel + AsPrimitive<To>,
            To: Pixel + FromPrimitive<From>,
        {
            let data: Vec<RasterTile2D<To>> = raster_tiles
                .iter()
                .cloned()
                .map(RasterTile2D::convert)
                .collect();
            MockRasterSourceProcessor::new(data).boxed()
        }

        Ok(call_generic_raster_processor!(
            self.result_descriptor().data_type,
            converted(&self.data)
        ))
    }

    fn result_descriptor(&self) -> &RasterResultDescriptor {
        &self.result_descriptor
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::MockExecutionContext;
    use geoengine_datatypes::primitives::Measurement;
    use geoengine_datatypes::raster::RasterDataType;
    use geoengine_datatypes::{
        primitives::TimeInterval,
        raster::{Grid2D, TileInformation},
        spatial_reference::SpatialReference,
    };

    #[tokio::test]
    async fn serde() {
        let no_data_value = None;
        let raster = Grid2D::new([3, 2].into(), vec![1, 2, 3, 4, 5, 6], no_data_value).unwrap();

        let raster_tile = RasterTile2D::new_with_tile_info(
            TimeInterval::default(),
            TileInformation {
                global_geo_transform: Default::default(),
                global_tile_position: [0, 0].into(),
                tile_size_in_pixels: [3, 2].into(),
            },
            raster.into(),
        );

        let mrs = MockRasterSource {
            params: MockRasterSourceParams {
                data: vec![raster_tile],
                result_descriptor: RasterResultDescriptor {
                    data_type: RasterDataType::U8,
                    spatial_reference: SpatialReference::epsg_4326().into(),
                    measurement: Measurement::Unitless,
                    no_data_value: no_data_value.map(AsPrimitive::as_),
                },
            },
        }
        .boxed();

        let serialized = serde_json::to_string(&mrs).unwrap();

        let spec = serde_json::json!({
            "type": "MockRasterSource",
            "params": {
                "data": [{
                    "time": {
                        "start": -8_334_632_851_200_000_i64,
                        "end": 8_210_298_412_799_999_i64
                    },
                    "tilePosition": [0, 0],
                    "globalGeoTransform": {
                        "originCoordinate": {
                            "x": 0.0,
                            "y": 0.0
                        },
                        "xPixelSize": 1.0,
                        "yPixelSize": -1.0
                    },
                    "gridArray": {
                        "type": "grid",
                        "shape": {
                            "shapeArray": [3, 2]
                        },
                        "data": [1, 2, 3, 4, 5, 6],
                        "noDataValue": null
                    },
                    "properties":{
                        "scale":null,
                        "offset":null,
                        "band_name":null,
                        "properties_map":{}
                    }
                }],
                "resultDescriptor": {
                    "dataType": "U8",
                    "spatialReference": "EPSG:4326",
                    "measurement": {
                        "type": "unitless"
                    },
                    "noDataValue": null
                }
            }
        })
        .to_string();
        assert_eq!(serialized, spec);

        let deserialized: Box<dyn RasterOperator> = serde_json::from_str(&serialized).unwrap();

        let execution_context = MockExecutionContext::default();

        let initialized = deserialized.initialize(&execution_context).await.unwrap();

        match initialized.query_processor().unwrap() {
            crate::engine::TypedRasterQueryProcessor::U8(..) => {}
            _ => panic!("wrong raster type"),
        }
    }
}
