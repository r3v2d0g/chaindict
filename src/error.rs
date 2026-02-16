use std::fmt::{self, Display, Formatter};

use crate::{LinkId, storage::Kind};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    /// The chain is disconnected.
    ///
    /// When loading the links start from `latest` and going backward, we should
    /// eventually reach `expected`, but instead we reach the end of the chain at `got`.
    Disconnected {
        latest: LinkId,
        expected: LinkId,
        got: LinkId,
    },

    /// The file of the given kind for the link with the given ID does not exist
    /// although it should.
    DoesNotExist { link: LinkId, kind: Kind },

    /// The newly created link is empty, which isn't allowed.
    Empty,

    /// The file is smaller than expected.
    FileSize { expected: usize, got: usize },

    /// A snapshot file cannot be created because entries were already added to the
    /// delta for the link.
    NotEmpty,

    /// An error occurred while interacting with the storage.
    Storage(opendal::Error),

    /// The maximum number of entries ([`u32::MAX`]) has been reached, no new entry can
    /// be inserted.
    TooManyEntries,

    /// The storage format version used to encode a file is unsupported.
    Version { expected: u16, got: u16 },
}

impl From<opendal::Error> for Error {
    #[inline]
    fn from(error: opendal::Error) -> Self {
        Self::Storage(error)
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            Self::Disconnected {
                latest,
                expected,
                got,
            } => write!(
                f,
                "Disconnected chain: while loading from {latest}, expected to reach {expected} but ended up at {got}"
            ),

            Self::DoesNotExist { link, kind } => write!(f, "File does not exist: {link}.{kind}"),
            Self::Empty => write!(f, "Link is empty"),
            Self::FileSize { expected, got } => write!(
                f,
                "File is too small: expected >= {expected} bytes but it only contains {got} bytes"
            ),

            Self::NotEmpty => write!(f, "Cannot create a snapshot with a non-empty delta"),
            Self::Storage(error) => write!(f, "{error}"),
            Self::TooManyEntries => write!(f, "Reached the maximum number of entries"),

            Self::Version { expected, got } => write!(
                f,
                "Unsupported storage format version: expected {expected} but file was encoded with {got}"
            ),
        }
    }
}
