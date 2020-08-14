use crate::error::{ArrowWrappedError, Error};
use arrow::error::ArrowError;
use snafu::Snafu;

#[derive(Debug, PartialEq, Snafu)]
#[snafu(visibility = "pub(crate)")]
pub enum FeatureCollectionError {
    #[snafu(display("Arrow internal error: {:?}", source))]
    ArrowInternal {
        source: ArrowWrappedError,
    },

    CannotAccessReservedColumn {
        name: String,
    },

    ColumnDoesNotExist {
        name: String,
    },

    ColumnAlreadyExists {
        name: String,
    },

    UnmatchedLength {
        a: usize,
        b: usize,
    },

    UnmatchedSchema {
        a: Vec<String>,
        b: Vec<String>,
    },

    WrongDataType,
}

impl From<FeatureCollectionError> for Error {
    fn from(error: FeatureCollectionError) -> Self {
        Error::FeatureCollection { source: error }
    }
}

impl From<ArrowError> for FeatureCollectionError {
    fn from(source: ArrowError) -> Self {
        FeatureCollectionError::ArrowInternal {
            source: source.into(),
        }
    }
}
