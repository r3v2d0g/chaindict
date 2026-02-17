//! ## `chaindict`
//!
//! A chain of dictionaries.
//!
//! This crate implements a storage format which allows to create chains of
//! dictionaries (called links), each extending the previous links in the chain by
//! adding new unique entries which are each mapped to a unique `u32`.
//!
//! Links are stored with snapshot and/or delta files, where snapshots contain the
//! list of all entries which have been inserted into this link and the previous
//! ones, and deltas only contain the list of entries which have been inserted into
//! this link.
//!
//! The goals are:
//! 1. to maintain a dictionary of values, mapping those to `u32`s; and
//! 2. to allow using a storage backend like S3 (i.e. where files cannot be modified)
//!    to maintain this dictionary; and
//! 3. to still be able to extend the dictionary at any point with new entries; and
//! 4. to efficiently get all of the entries from the storage backend; and
//! 5. to efficiently get only the new entries from the storage backend.

use std::{
    fmt::{self, Debug, Display, Formatter},
    hash::Hash,
};

pub(crate) use self::{
    delta::Footer as DFooter, entries::Entries, snapshot::Footer as SFooter, storage::Storage,
};

use uuid::Uuid;

mod delta;
mod entries;
mod error;
mod reader;
mod snapshot;
mod writer;

pub mod storage;

pub use self::{
    error::{Error, Result},
    reader::Reader,
    writer::{LazyWriter, Writer},
};

/// The ID of a link in a chain, extending all previous links (unless it is the
/// first one) with new entries.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct LinkId(Uuid);

/// An entry which can be inserted into a chain of links.
///
/// Each unique entry will have a unique `u32` assigned to it, so that entries can
/// be mapped to `u32`s, and `u32`s can be mapped back to entries.
#[trait_variant::make(Send)]
pub trait Entry: Eq + Hash + Sized {
    /// The size of an entry when encoded.
    const SIZE: usize;

    /// Reads an entry from the given reader.
    ///
    /// This _must_ read exactly `SIZE` bytes.
    async fn read(reader: &mut storage::Reader) -> Result<Self>;

    /// Writes the entry to the given writer.
    ///
    /// This _must_ write exactly `SIZE` bytes.
    async fn write(&self, writer: &mut storage::Writer) -> Result<()>;
}

impl LinkId {
    /// Generates a new random link ID.
    #[inline]
    pub(crate) fn random() -> Self {
        // TODO(MLB): use a time-sorted UUID?
        Self(Uuid::new_v4())
    }

    /// Converts the given `u128` to a link ID.
    #[inline]
    pub(crate) fn from_u128(num: u128) -> Self {
        Self(Uuid::from_u128(num))
    }

    /// Converts the link ID to a `u128`.
    #[inline]
    pub(crate) fn as_u128(&self) -> u128 {
        self.0.as_u128()
    }
}

impl Debug for LinkId {
    #[inline]
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "LinkId({})", self.0)
    }
}

impl Display for LinkId {
    #[inline]
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}
