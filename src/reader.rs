use std::hash::{BuildHasher, RandomState};

use crate::{DFooter, Entries, Entry, Error, LinkId, Result, SFooter, Storage, storage::Kind::*};

/// A reader which allows getting the entries of a chain stored in some storage.
pub struct Reader<T: Entry, S = RandomState> {
    /// The storage containing the chain's links.
    storage: Storage,

    /// The ID of the latest link which has been loaded.
    latest: LinkId,

    /// The entries which have been loaded.
    entries: Entries<T, S>,
}

impl<T: Entry, S: BuildHasher + Default> Reader<T, S> {
    /// Creates a new reader from the given storage, loading the necessary links' files.
    ///
    /// `latest` is the latest link in the chain, such that [`get_at()`][1] and
    /// [`get_index_of()`][2] will work for any entry which has been inserted in that
    /// link or any previous link.
    ///
    /// [1]: Self::get_at()
    /// [2]: Self::get_index_of()
    pub async fn open(latest: LinkId, storage: Storage) -> Result<Self> {
        // TODO(MLB): if the snapshot doesn't exist, load the deltas instead until a snapshot exists
        let mut reader = storage.open(latest, Snapshot).await?;
        let footer = SFooter::read(&mut reader).await?;

        let mut entries = Entries::with_capacity(footer.count as usize);

        for _ in 0..footer.count {
            let entry = T::read(&mut reader).await?;
            entries.insert_unique(entry);
        }

        Ok(Self {
            storage,

            latest,
            entries,
        })
    }

    /// Reloads the reader so that all of the entries present in the `latest` link can
    /// be used.
    pub async fn reload(&mut self, latest: LinkId) -> Result<()> {
        let mut deltas = Vec::new();

        let mut next = latest;
        while next != self.latest {
            let mut reader = self.storage.open(next, Delta).await?;
            let footer = DFooter::read(&mut reader).await?;

            let Some(previous) = footer.previous else {
                return Err(Error::Disconnected {
                    latest,
                    expected: self.latest,
                    got: next,
                });
            };

            let mut delta = Vec::with_capacity(footer.count as usize);
            for _ in 0..footer.count {
                // TODO(MLB): validate that exactly `T::SIZE` bytes were read
                let entry = T::read(&mut reader).await?;

                delta.push(entry);
            }

            deltas.push(delta);
            next = previous;
        }

        // TODO(MLB): allow to optionally "layer" the deltas instead of merging them
        for delta in deltas.into_iter().rev() {
            for entry in delta {
                self.entries.insert_unique(entry);
            }
        }

        self.latest = latest;

        Ok(())
    }

    /// Returns the number of entries present.
    #[inline]
    #[allow(clippy::len_without_is_empty)] // `is_empty` would otherwise always return `false`
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns the entry represented by the given `u32`, if there is one.
    #[inline]
    pub fn get_at(&self, index: u32) -> Option<&T> {
        // TODO(MLB): optionally be lazy and only load when this is called
        // TODO(MLB): if lazy, load the entries in blocks to amortize
        // TODO(MLB): also, potentially pre-allocate or chunk the `Entries`

        self.entries.get_at(index)
    }

    /// Returns the `u32` assigned to the given `entry`, if it is present.
    #[inline]
    pub fn get_index_of(&self, entry: &T) -> Option<u32> {
        // NOTE(MLB): if `get_at()` becomes lazy, this cannot or at least it'll require
        //            loading all of the entries
        self.entries.get_index_of(entry)
    }
}
