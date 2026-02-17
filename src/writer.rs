use std::marker::PhantomData;

use futures::future::try_join;

use crate::{
    DFooter, Entry, Error, LinkId, Result, Storage,
    snapshot::Footer as SFooter,
    storage::{self, Kind::*},
};

mod lazy;

pub use self::lazy::LazyWriter;

/// A writer which allows adding entries to a chain stored in some storage by
/// creating a new link.
pub struct Writer<T: Entry> {
    storage: Storage,

    /// The offset of the first entry inserted as part of the link this is creating.
    offset: u32,

    /// The total number of entries in the chain, including those which have been
    /// inserted as part of the link this is creating.
    count: u32,

    /// The ID of the link this is creating.
    id: LinkId,

    /// The ID of the previous link in the chain, which the link this is creating is
    /// extending.
    previous: Option<LinkId>,

    /// The index of the link this is creating in the chain of links.
    index: u32,

    /// The writer for the delta file for the link this is creating.
    delta: storage::Writer,

    /// The writer for the snapshot file for the link this is creating.
    snapshot: Option<storage::Writer>,

    _t: PhantomData<T>,
}

impl<T: Entry> Writer<T> {
    /// Creates a new writer for the given storage, creating a link which is extending
    /// `previous`.
    pub async fn create(previous: Option<LinkId>, storage: Storage) -> Result<Self> {
        let id = LinkId::random();
        let delta = storage.create(id, Delta).await?;

        Ok(Self {
            storage,

            offset: 0,
            count: 0,

            id,
            previous,
            index: 0,

            delta,
            snapshot: None,

            _t: PhantomData,
        })
    }

    /// Writes a snapshot file for the link.
    ///
    /// Fails if entries have already been added to the link's delta file.
    pub async fn with_snapshot(&mut self) -> Result<()> {
        // TODO(MLB): optionally start loading snapshot in background

        if self.delta.file_size() != 0 {
            return Err(Error::NotEmpty);
        }

        let mut snapshot = self.storage.create(self.id, Snapshot).await?;
        if let Some(previous) = self.previous {
            // TODO(MLB): if append is supported, copy the file then append to it (ignoring the footer in the middle when reading)
            // TODO(MLB): read + start writing in the background, buffering while preparing

            let mut previous = self.storage.open(previous, Snapshot).await?;
            let footer = SFooter::read(&mut previous).await?;
            snapshot.copy_from(previous).await?;

            self.offset = footer.count;
            self.count = footer.count;
            self.index = footer.index + 1;
        }

        self.snapshot = Some(snapshot);

        Ok(())
    }

    /// Writes a unique entry to the link's file(s), returning the `u32` assigned to it.
    ///
    /// The caller _must_ guarantee that the entry has not been inserted in a previous
    /// link.
    pub async fn write_unique(&mut self, entry: T) -> Result<u32> {
        // If `previous` has been set but `index` is still `0`, it means that we are not
        // writing a snapshot file (i.e. `with_snapshot()` hasn't been called) – we need to
        // read the previous link's delta footer to get some information about the state of
        // the chain.
        if self.index == 0
            && let Some(previous) = self.previous
        {
            let mut previous = self.storage.open(previous, Delta).await?;
            let footer = DFooter::read(&mut previous).await?;

            self.offset = footer.count;
            self.count = footer.count;
            self.index = footer.index + 1;
        }

        if self.count == u32::MAX {
            return Err(Error::TooManyEntries);
        }

        let id = self.count;
        self.count += 1;

        // TODO(MLB): validate that exactly `T::SIZE` bytes were written
        entry.write(&mut self.delta).await?;
        if let Some(snapshot) = &mut self.snapshot {
            entry.write(snapshot).await?;
        }

        Ok(id)
    }

    /// Finishes writing, flushing all remaining bytes to the file(s) and retuning the
    /// ID assigned to the newly created link.
    ///
    /// Fails if no entries were added to the link.
    pub async fn finish(self) -> Result<LinkId> {
        let Self {
            offset,
            count,
            id,
            previous,
            index,
            mut delta,
            snapshot,
            ..
        } = self;

        if offset == count {
            return Err(Error::Empty);
        }

        let dfooter = DFooter {
            previous,
            index,
            total: count,
            count: count - offset,
        };

        let sfooter = SFooter {
            previous,
            index,
            count,
        };

        let delta = async move {
            dfooter.write(&mut delta).await?;
            delta.finish().await
        };

        let snapshot = async move {
            if let Some(mut snapshot) = snapshot {
                sfooter.write(&mut snapshot).await?;
                snapshot.finish().await
            } else {
                Ok(())
            }
        };

        try_join(delta, snapshot).await?;

        Ok(id)
    }
}
