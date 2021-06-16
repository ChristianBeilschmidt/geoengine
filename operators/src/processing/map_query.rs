use crate::engine::{QueryContext, QueryProcessor};
use crate::util::Result;
use async_trait::async_trait;
use futures::stream::BoxStream;

/// This `QueryProcessor` allows to rewrite a query. It does not change the data. Results of the children are forwarded.
pub(crate) struct MapQueryProcessor<S, Q> {
    source: S,
    query_fn: Q,
}

impl<S, Q> MapQueryProcessor<S, Q> {
    pub fn new(source: S, query_fn: Q) -> Self {
        Self { source, query_fn }
    }
}

#[async_trait]
impl<S, Q> QueryProcessor for MapQueryProcessor<S, Q>
where
    S: QueryProcessor,
    Q: Fn(S::Qrect) -> Result<S::Qrect> + Sync + Send,
{
    type Output = S::Output;
    type Qrect = S::Qrect;

    async fn query<'a>(
        &'a self,
        query: S::Qrect,
        ctx: &'a dyn QueryContext,
    ) -> Result<BoxStream<'a, Result<Self::Output>>> {
        let rewritten_query = (self.query_fn)(query)?;
        self.source.query(rewritten_query, ctx).await
    }
}
