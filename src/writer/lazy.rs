use super::Writer;
use crate::{Entry, Error, LinkId, Result, Storage};

/// A lazy version of [`Writer`] which only creates new files when first trying to
/// write new entries.
pub struct LazyWriter<T: Entry> {
    state: State<T>,
}

#[expect(clippy::large_enum_variant)]
enum State<T: Entry> {
    Uncreated {
        previous: Option<LinkId>,
        storage: Storage,
        snapshot: bool,
    },

    Created {
        writer: Writer<T>,
    },
}

impl<T: Entry> LazyWriter<T> {
    /// Creates a new lazy writer for the given storage, lazily creating a link which
    /// will extend `previous`.
    ///
    /// Contrarily to [`Writer`], this only creates new file on the first call to
    /// [`write_unique()`].
    pub fn create(previous: Option<LinkId>, storage: Storage) -> Self {
        let state = State::Uncreated {
            previous,
            storage,
            snapshot: false,
        };

        Self { state }
    }

    /// Indicates that the writer should create a snapshot file for the link.
    ///
    /// Fails if entries have already been added to the link's delta file.
    pub fn with_snapshot(&mut self) -> Result<()> {
        match &mut self.state {
            State::Uncreated { snapshot, .. } => {
                *snapshot = true;

                Ok(())
            }

            State::Created { .. } => Err(Error::NotEmpty),
        }
    }

    // TODO(MLB): with_snapshot_from

    /// Writes a unique entry to the link's file(s), returning the `u32` assigned to it.
    ///
    /// The caller _must_ guarantee that the entry has not been inserted in a previous
    /// link.
    ///
    /// If the new files for the link have not been created yet, this creates them.
    #[inline]
    pub async fn write_unique(&mut self, entry: T) -> Result<u32> {
        let writer = self.state.make_created().await?;
        writer.write_unique(entry).await
    }
}

impl<T: Entry> State<T> {
    /// Ensures that a [`Writer`] has been created, returning a mutable reference to it.
    async fn make_created(&mut self) -> Result<&mut Writer<T>> {
        if let Self::Uncreated {
            previous,
            storage,
            snapshot,
        } = self
        {
            // TODO(MLB): don't clone `storage`
            let mut writer = Writer::create(*previous, storage.clone()).await?;
            if *snapshot {
                writer.with_snapshot().await?;
            }

            *self = Self::Created { writer };
        }

        let Self::Created { writer } = self else {
            unreachable!()
        };

        Ok(writer)
    }
}
