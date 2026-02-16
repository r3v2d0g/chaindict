use std::hash::{BuildHasher, RandomState};

use hashbrown::HashTable;

use crate::Entry;

/// A set of unique entries, each with a `u32` assigned to them.
///
/// This is like an `IndexSet`, but using `u32`s instead of `usize`s.
pub struct Entries<T: Entry, S = RandomState> {
    /// Maps the hashes of the entries in `entries` to their index in it.
    indexes: HashTable<u32>,

    /// Stores the actual entries which were inserted into the set.
    // TODO(MLB): optionally cache the hash
    entries: Vec<T>,

    /// The hasher used to determine where the entries' index should be stored in
    /// `indexes`.
    hasher: S,
}

impl<T: Entry, S: BuildHasher + Default> Entries<T, S> {
    /// Creates a new [`Entries`] with enough capacity to insert at least `capacity`
    /// entries.
    #[inline]
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            indexes: HashTable::with_capacity(capacity),
            entries: Vec::with_capacity(capacity),
            hasher: S::default(),
        }
    }

    /// Returns the number of entries present.
    #[inline]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns the entry represented by the given `u32`, if there is one.
    #[inline]
    pub fn get_at(&self, index: u32) -> Option<&T> {
        self.entries.get(index as usize)
    }

    /// Returns the `u32` assigned to the given `entry`, if it has been inserted.
    #[inline]
    pub fn get_index_of(&self, entry: &T) -> Option<u32> {
        let hash = self.hasher.hash_one(entry);
        let eq = |index: &u32| entry == &self.entries[*index as usize];

        self.indexes.find(hash, eq).copied()
    }

    /// Inserts a new entry which isn't already present.
    ///
    /// The caller _must_ guarantee that the entry has not been inserted already.
    pub fn insert_unique(&mut self, entry: T) -> u32 {
        let hash = self.hasher.hash_one(&entry);
        let index = self.entries.len() as u32;
        let hasher = |index: &u32| {
            let entry = &self.entries[*index as usize];
            self.hasher.hash_one(entry)
        };

        self.indexes.insert_unique(hash, index, hasher);
        self.entries.push(entry);

        index
    }
}

impl<T: Entry, S: Default> Default for Entries<T, S> {
    #[inline]
    fn default() -> Self {
        Self {
            indexes: HashTable::default(),
            entries: Vec::default(),
            hasher: S::default(),
        }
    }
}
