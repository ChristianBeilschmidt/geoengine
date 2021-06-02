use std::collections::HashMap;

use crate::users::user::UserId;
use crate::{datasets::listing::DatasetListOptions, error::Result};
use crate::{
    datasets::{
        listing::{DatasetListing, DatasetProvider},
        storage::{DatasetDefinition, DatasetProviderDefinition, MetaDataDefinition},
    },
    error,
    util::user_input::Validated,
};
use async_trait::async_trait;
use chrono::Duration;
use geoengine_datatypes::dataset::{DatasetId, DatasetProviderId, ExternalDatasetId};
use geoengine_datatypes::primitives::Measurement;
use geoengine_datatypes::raster::{Raster, RasterDataType};
use geoengine_datatypes::spatial_reference::SpatialReference;
use geoengine_operators::engine::QueryRectangle;
use geoengine_operators::{
    engine::{MetaData, MetaDataProvider, RasterResultDescriptor, VectorResultDescriptor},
    mock::MockDatasetDataSourceLoadingInfo,
    source::{GdalLoadingInfo, OgrSourceDataset},
};
use log::{debug, info};
use serde::{Deserialize, Serialize};
use snafu::ResultExt;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SentinelS2L2ACogsProviderDefinition {
    name: String,
    id: DatasetProviderId,
    api_url: String,
}

#[typetag::serde]
impl DatasetProviderDefinition for SentinelS2L2ACogsProviderDefinition {
    fn initialize(
        self: Box<Self>,
    ) -> crate::error::Result<Box<dyn crate::datasets::listing::DatasetProvider>> {
        Ok(Box::new(SentinelS2L2aCogsDataProvider::new(
            self.id,
            self.api_url,
        )))
    }

    fn type_name(&self) -> String {
        "SentinelS2L2ACogs".to_owned()
    }

    fn name(&self) -> String {
        self.name.clone()
    }

    fn id(&self) -> DatasetProviderId {
        self.id
    }
}

#[derive(Debug, Clone)]
pub struct Band {
    pub name: String,
    pub no_data_value: Option<f64>,
    pub data_type: RasterDataType,
}

impl Band {
    pub fn new(name: String, no_data_value: Option<f64>, data_type: RasterDataType) -> Self {
        Self {
            name,
            no_data_value,
            data_type,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Zone {
    pub name: String,
    pub epsg: u32,
}

impl Zone {
    pub fn new(name: String, epsg: u32) -> Self {
        Self { name, epsg }
    }
}

#[derive(Debug, Clone)]
pub struct SentinelMetaData {
    bands: Vec<Band>,
    zones: Vec<Zone>,
}

#[derive(Debug, Clone)]
pub struct SentinelDataset {
    band: Band,
    zone: Zone,
    listing: DatasetListing,
}

pub struct SentinelS2L2aCogsDataProvider {
    id: DatasetProviderId,
    api_url: String,
    meta_data: SentinelMetaData,
    datasets: HashMap<DatasetId, SentinelDataset>,
}

impl SentinelS2L2aCogsDataProvider {
    pub fn new(id: DatasetProviderId, api_url: String) -> Self {
        let meta_data = Self::load_metadata();
        Self {
            id,
            api_url,
            datasets: Self::create_datasets(&id, &meta_data),
            meta_data,
        }
    }

    fn load_metadata() -> SentinelMetaData {
        // TODO: fetch dataset metadata from config or remote
        // TODO: is there a no data value?
        SentinelMetaData {
            bands: vec![
                Band::new("B01".to_owned(), None, RasterDataType::U16),
                Band::new("B02".to_owned(), None, RasterDataType::U16),
            ],
            zones: vec![
                Zone::new("UTM32N".to_owned(), 32632),
                Zone::new("UTM36S".to_owned(), 32736),
            ],
        }
    }

    fn create_datasets(
        id: &DatasetProviderId,
        meta_data: &SentinelMetaData,
    ) -> HashMap<DatasetId, SentinelDataset> {
        meta_data
            .zones
            .iter()
            .flat_map(|zone| {
                meta_data.bands.iter().map(move |band| {
                    let dataset_id: DatasetId = ExternalDatasetId {
                        provider: *id,
                        id: format!("{}:{}", zone.name, band.name),
                    }
                    .into();
                    let listing = DatasetListing {
                        id: dataset_id.clone(),
                        name: format!("Sentinal S2 L2A COGS {}:{}", zone.name, band.name),
                        description: "".to_owned(),
                        tags: vec![],
                        source_operator: "GdalSource".to_owned(),
                        result_descriptor: RasterResultDescriptor {
                            data_type: band.data_type,
                            spatial_reference: SpatialReference::epsg(zone.epsg).into(),
                            measurement: Measurement::Unitless,
                            no_data_value: band.no_data_value,
                        }
                        .into(),
                    };

                    let dataset = SentinelDataset {
                        zone: zone.clone(),
                        band: band.clone(),
                        listing,
                    };

                    (dataset_id, dataset)
                })
            })
            .collect()
    }
}

#[async_trait]
impl DatasetProvider for SentinelS2L2aCogsDataProvider {
    async fn list(
        &self,
        _user: UserId,
        _options: Validated<DatasetListOptions>,
    ) -> Result<Vec<DatasetListing>> {
        // TODO: permission and options
        Ok(self.datasets.values().map(|d| d.listing.clone()).collect())
    }

    async fn load(
        &self,
        _user: crate::users::user::UserId,
        _dataset: &geoengine_datatypes::dataset::DatasetId,
    ) -> crate::error::Result<crate::datasets::storage::Dataset> {
        Err(error::Error::NotYetImplemented)
    }
}

#[derive(Debug, Clone)]
pub struct SentinelS2L2aCogsMetaData {
    api_url: String,
    zone: Zone,
    band: Band,
}

impl SentinelS2L2aCogsMetaData {
    async fn web<T: Serialize + ?Sized>(
        &self,
        params: &T,
    ) -> geoengine_operators::util::Result<String> {
        let client = reqwest::Client::new();
        client
            .get(&self.api_url)
            .query(&params)
            .send()
            .await
            .map_err(|_| geoengine_operators::error::Error::LoadingInfo)?
            .text()
            .await
            .map_err(|_| geoengine_operators::error::Error::LoadingInfo)
    }
}

impl MetaData<GdalLoadingInfo, RasterResultDescriptor> for SentinelS2L2aCogsMetaData {
    fn loading_info(
        &self,
        query: QueryRectangle,
    ) -> geoengine_operators::util::Result<GdalLoadingInfo> {
        // for reference: https://stacspec.org/STAC-ext-api.html#operation/getSearchSTAC

        let t_start = query.time_interval.start().as_utc_date_time().ok_or(
            geoengine_operators::error::Error::DataType {
                source: geoengine_datatypes::error::Error::NoDateTimeValid {
                    time_instance: query.time_interval.start(),
                },
            },
        )?;

        // shift start by 1 day to ensure getting the most recent data for start time
        let t_start = t_start - Duration::days(1);

        let t_end = query.time_interval.end().as_utc_date_time().ok_or(
            geoengine_operators::error::Error::DataType {
                source: geoengine_datatypes::error::Error::NoDateTimeValid {
                    time_instance: query.time_interval.end(),
                },
            },
        )?;

        let params = [
            ("collections[]", "sentinel-s2-l2a-cogs"),
            (
                "bbox",
                &format!(
                    "{},{},{},{}",
                    query.bbox.lower_left().x,
                    query.bbox.lower_left().y,
                    query.bbox.upper_right().x,
                    query.bbox.upper_right().y
                ),
            ), // TODO: order of coordinates
            (
                "datetime",
                &format!("{}/{}", t_start.to_rfc3339(), t_end.to_rfc3339()),
            ),
        ];

        info!("params {:?}", params);

        let result = futures::executor::block_on(self.web(&params));

        debug!("result {:?}", result);

        Err(geoengine_operators::error::Error::NotImplemented)
    }

    fn result_descriptor(&self) -> geoengine_operators::util::Result<RasterResultDescriptor> {
        Ok(RasterResultDescriptor {
            data_type: self.band.data_type,
            spatial_reference: SpatialReference::epsg(self.zone.epsg).into(),
            measurement: Measurement::Unitless,
            no_data_value: self.band.no_data_value,
        })
    }

    fn box_clone(&self) -> Box<dyn MetaData<GdalLoadingInfo, RasterResultDescriptor>> {
        Box::new(self.clone())
    }
}

impl MetaDataProvider<GdalLoadingInfo, RasterResultDescriptor> for SentinelS2L2aCogsDataProvider {
    fn meta_data(
        &self,
        dataset: &DatasetId,
    ) -> Result<
        Box<dyn MetaData<GdalLoadingInfo, RasterResultDescriptor>>,
        geoengine_operators::error::Error,
    > {
        let dataset = self
            .datasets
            .get(&dataset)
            .ok_or(geoengine_operators::error::Error::UnknownDatasetId)?;

        Ok(Box::new(SentinelS2L2aCogsMetaData {
            api_url: self.api_url.clone(),
            zone: dataset.zone.clone(),
            band: dataset.band.clone(),
        }))
    }
}

impl MetaDataProvider<MockDatasetDataSourceLoadingInfo, VectorResultDescriptor>
    for SentinelS2L2aCogsDataProvider
{
    fn meta_data(
        &self,
        _dataset: &DatasetId,
    ) -> Result<
        Box<dyn MetaData<MockDatasetDataSourceLoadingInfo, VectorResultDescriptor>>,
        geoengine_operators::error::Error,
    > {
        Err(geoengine_operators::error::Error::NotImplemented)
    }
}

impl MetaDataProvider<OgrSourceDataset, VectorResultDescriptor> for SentinelS2L2aCogsDataProvider {
    fn meta_data(
        &self,
        _dataset: &DatasetId,
    ) -> Result<
        Box<dyn MetaData<OgrSourceDataset, VectorResultDescriptor>>,
        geoengine_operators::error::Error,
    > {
        Err(geoengine_operators::error::Error::NotImplemented)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn web() -> geoengine_operators::util::Result<String> {
        let client = reqwest::Client::new();
        client
            .get("http://www.michaelmattig.de")
            .send()
            .await
            .map_err(|_| geoengine_operators::error::Error::LoadingInfo)?
            .text()
            .await
            .map_err(|_| geoengine_operators::error::Error::LoadingInfo)
    }

    fn foo() {
        let r = futures::executor::block_on(web());
        eprintln!("aaa{:?}", r);
    }

    #[tokio::test]
    async fn test_name() {
        tokio::task::spawn_blocking(foo).await.unwrap();
    }
}
