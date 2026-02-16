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
        let mut entries = Entries::default();
        let mut deltas = Vec::new();
        let mut total = 0;

        let mut next = latest;
        loop {
            // Snapshot files do not neccessarily exist – they are optional.
            //
            // We load all deltas until we either reach the end of the chain or a snapshot.
            if let Some(mut reader) = storage.open_maybe(next, Snapshot).await? {
                let footer = SFooter::read(&mut reader).await?;
                total += footer.count as usize;

                entries.reserve(total);

                for _ in 0..footer.count {
                    let entry = T::read(&mut reader).await?;
                    entries.insert_unique(entry);
                }

                break;
            }

            // If no snapshot exists for the link, we instead try to load the delta for it.
            let mut reader = storage.open(next, Delta).await?;
            let footer = DFooter::read(&mut reader).await?;

            let mut delta = Vec::with_capacity(footer.count as usize);
            total += footer.count as usize;

            for _ in 0..footer.count {
                // TODO(MLB): validate that exactly `T::SIZE` bytes were read
                let entry = T::read(&mut reader).await?;

                delta.push(entry);
            }

            deltas.push(delta);

            // Unless this is the last link in the chain we try to load the previous one.
            let Some(previous) = footer.previous else {
                break;
            };

            next = previous;
        }

        // `entries` is empty if only read deltas – it should otherwise contain some
        // entries. If it is empty, then we reserve some capacity. If it isn't it should
        // already have enough capacity to insert all of the entries in `deltas`.
        if entries.is_empty() {
            entries.reserve(total);
        }

        for delta in deltas.into_iter().rev() {
            for entry in delta {
                entries.insert_unique(entry);
            }
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
        let mut additional = 0;

        // TODO(MLB): set a threshold above which we try loading a snapshot (i.e. if there are more
        //            than `N` entries to load or more than `M` deltas)

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
            additional += footer.count as usize;

            for _ in 0..footer.count {
                // TODO(MLB): validate that exactly `T::SIZE` bytes were read
                let entry = T::read(&mut reader).await?;

                delta.push(entry);
            }

            deltas.push(delta);
            next = previous;
        }

        self.entries.reserve(additional);

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
