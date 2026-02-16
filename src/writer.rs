use std::marker::PhantomData;

use futures::future::try_join;

use crate::{
    DFooter, Entry, Error, LinkId, Result, Storage,
    snapshot::Footer as SFooter,
    storage::{self, Kind::*},
};

/// A writer which allows adding entries to a chain stored in some storage by
/// creating a new link.
pub struct Writer<T: Entry> {
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
    // TODO(MLB): make optional
    snapshot: storage::Writer,

    _t: PhantomData<T>,
}

impl<T: Entry> Writer<T> {
    /// Creates a new writer for the given storage, creating a link which is extending
    /// `previous`.
    pub async fn create(previous: Option<LinkId>, storage: Storage) -> Result<Self> {
        // TODO(MLB): get latest
        // TODO(MLB): optionally start loading snapshot in background

        let id = LinkId::random();
        let delta = storage.create(id, Delta).await?;
        let mut snapshot = storage.create(id, Snapshot).await?;

        let (offset, index) = if let Some(previous) = previous {
            // TODO(MLB): if append is supported, copy the file then append to it (ignoring the footer in the middle when reading)
            // TODO(MLB): read + start writing in the background, buffering while preparing

            let mut previous = storage.open(previous, Snapshot).await?;
            let footer = SFooter::read(&mut previous).await?;
            snapshot.copy_from(previous).await?;

            (footer.count, footer.index + 1)
        } else {
            (0, 0)
        };

        Ok(Self {
            offset,
            count: 0,

            id,
            previous,
            index,

            delta,
            snapshot,

            _t: PhantomData,
        })
    }

    /// Writes a unique entry to the link's file(s), returning the `u32` assigned to it.
    ///
    /// The caller _must_ guarantee that the entry has not been inserted in a previous
    /// link.
    pub async fn write_unique(&mut self, entry: T) -> Result<u32> {
        if self.count == u32::MAX {
            return Err(Error::TooManyEntries);
        }

        let id = self.count;
        self.count += 1;

        // TODO(MLB): validate that exactly `T::SIZE` bytes were written
        entry.write(&mut self.delta).await?;
        entry.write(&mut self.snapshot).await?;

        Ok(id)
    }

    /// Finishes writing, flushing all remaining bytes to the file(s) and retuning the
    /// ID assigned to the newly created link.
    pub async fn finish(self) -> Result<LinkId> {
        let Self {
            offset,
            count,
            id,
            previous,
            index,
            mut delta,
            mut snapshot,
            ..
        } = self;

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

        try_join(dfooter.write(&mut delta), sfooter.write(&mut snapshot)).await?;
        try_join(delta.finish(), snapshot.finish()).await?;

        Ok(id)
    }
}
