use std::{
    fmt::{self, Display, Formatter},
    ops::Range,
};

use futures::prelude::*;
use opendal::Operator;

use crate::{Error, LinkId, Result};

pub enum Kind {
    Delta,
    Snapshot,
}

pub struct Storage {
    base: Option<String>,
    operator: Operator,
}

/// A reader for a file which exists in some storage.
pub struct Reader {
    /// The current position inside of the file being read.
    offset: usize,

    /// The size of the file being read.
    ///
    /// This can be updated to make the file appear as smaller than it really is (e.g.
    /// to simplify reading all of a file's content but its footer).
    file_size: usize,

    /// The raw reader this is reading from.
    reader: opendal::Reader,
}

/// A writer for a file which was created in some storage.
pub struct Writer {
    /// The raw writer this is writing to.
    writer: opendal::Writer,
}

/// The (currently) latest version of the storage format.
///
/// This is used to make the storage format backward compatible at best, or to
/// fail on incompatibilities at worst.
pub(crate) const VERSION: u16 = 0;

impl Storage {
    /// Creates a new [`Storage`] from the given [`Operator`].
    pub fn new(operator: Operator) -> Self {
        Self {
            base: None,
            operator,
        }
    }

    /// Creates a new [`Storage`] from the given [`Operator`], using `base` as the base
    /// path for all of the files read and written.
    pub fn new_in(base: impl Into<String>, operator: Operator) -> Self {
        Self {
            base: Some(base.into()),
            operator,
        }
    }

    /// Opens the file of the given kind for the link with the given ID, returning a
    /// reader for it.
    pub(crate) async fn open(&self, id: LinkId, kind: Kind) -> Result<Reader> {
        let path = self.path(id, kind);

        let metadata = self.operator.stat(&path).await?;
        let file_size = metadata.content_length() as usize;

        // TODO(MLB): configure the reader?
        let reader = self.operator.reader(&path).await?;

        Ok(Reader {
            offset: 0,
            file_size,
            reader,
        })
    }

    /// Creates a file of the given kind for the link with the given ID, returning a
    /// writer for it.
    pub(crate) async fn create(&self, id: LinkId, kind: Kind) -> Result<Writer> {
        let path = self.path(id, kind);

        // TODO(MLB): configure the writer?
        let writer = self.operator.writer(&path).await?;

        Ok(Writer { writer })
    }

    /// Returns the path at which the file of the given kind for the link with the given
    /// ID should exist or be created.
    #[inline]
    fn path(&self, id: LinkId, kind: Kind) -> String {
        if let Some(base) = &self.base {
            format!("{base}/{id}.{kind}")
        } else {
            format!("{id}.{kind}")
        }
    }
}

impl Reader {
    #[inline]
    pub(crate) fn file_size(&self) -> usize {
        self.file_size
    }

    /// Reads a `u16` from the reader.
    ///
    /// This also updates the reader's current position accordingly.
    #[inline]
    pub async fn read_u16(&mut self) -> Result<u16> {
        let bytes = self.read_bytes().await?;
        Ok(u16::from_be_bytes(bytes))
    }

    /// Reads a `u32` from the reader.
    ///
    /// This also updates the reader's current position accordingly.
    #[inline]
    pub async fn read_u32(&mut self) -> Result<u32> {
        let bytes = self.read_bytes().await?;
        Ok(u32::from_be_bytes(bytes))
    }

    /// Reads a `u64` from the reader.
    ///
    /// This also updates the reader's current position accordingly.
    #[inline]
    pub async fn read_u64(&mut self) -> Result<u64> {
        let bytes = self.read_bytes().await?;
        Ok(u64::from_be_bytes(bytes))
    }

    /// Reads a `u128` from the reader.
    ///
    /// This also updates the reader's current position accordingly.
    #[inline]
    pub async fn read_u128(&mut self) -> Result<u128> {
        let bytes = self.read_bytes().await?;
        Ok(u128::from_be_bytes(bytes))
    }

    /// Reads a byte array of the given size from the reader.
    ///
    /// This also updates the reader's current position accordingly.
    pub async fn read_bytes<const N: usize>(&mut self) -> Result<[u8; N]> {
        let mut bytes = [0u8; N];
        let range = self.range(N)?;

        // TODO(MLB): do some buffering?
        self.reader
            .read_into(&mut bytes.as_mut_slice(), range)
            .await?;

        Ok(bytes)
    }

    /// Returns the range that should be used to read `len` bytes at the current
    /// position.
    ///
    /// Fails if the range would not be valid.
    #[inline]
    pub(crate) fn range(&self, len: usize) -> Result<Range<u64>> {
        // TODO(MLB): saturating add?
        if self.offset + len > self.file_size {
            return Err(Error::FileSize {
                expected: self.offset + len,
                // TODO(MLB): differentiate the "real" file size from the one set with `set_file_size()`
                got: self.file_size,
            });
        }

        Ok((self.offset as u64)..(self.offset + len) as u64)
    }

    /// Updates the current reader position based on `offset`.
    ///
    /// A positive `offset` value represents a value from the start of the file, whereas
    /// a negative one represents a value from the end of it (i.e. if `file_size = 10`
    /// and `offset = -1`, then the new position will be `9`).
    pub(crate) fn goto(&mut self, offset: isize) -> Result<()> {
        if offset.is_positive() {
            self.offset = offset as usize;
        } else {
            let Some(offset) = self.file_size.checked_add_signed(offset) else {
                // TODO(MLB): handle...
                todo!()
            };

            self.offset = offset;
        }

        Ok(())
    }

    pub(crate) fn set_file_size(&mut self, file_size: usize) {
        self.file_size = file_size;
    }
}

impl Writer {
    /// Reads everything from `reader` and writes it to the writer as-is.
    pub(crate) async fn copy_from(&mut self, reader: Reader) -> Result<()> {
        let range = (reader.offset as u64)..(reader.file_size as u64);
        let mut stream = reader.reader.into_stream(range).await?;

        while let Some(buffer) = stream.try_next().await? {
            self.writer.write(buffer).await?;
        }

        Ok(())
    }

    /// Writes a `u16` into the writer.
    #[inline]
    pub async fn write_u16(&mut self, value: u16) -> Result<()> {
        let bytes = value.to_be_bytes();
        self.write_bytes(bytes).await
    }

    /// Writes a `u32` into the writer.
    #[inline]
    pub async fn write_u32(&mut self, value: u32) -> Result<()> {
        let bytes = value.to_be_bytes();
        self.write_bytes(bytes).await
    }

    /// Writes a `u64` into the writer.
    #[inline]
    pub async fn write_u64(&mut self, value: u64) -> Result<()> {
        let bytes = value.to_be_bytes();
        self.write_bytes(bytes).await
    }

    /// Writes a `u128` into the writer.
    #[inline]
    pub async fn write_u128(&mut self, value: u128) -> Result<()> {
        let bytes = value.to_be_bytes();
        self.write_bytes(bytes).await
    }

    /// Writes the given bytes into the writer.
    pub async fn write_bytes<const N: usize>(&mut self, bytes: [u8; N]) -> Result<()> {
        // TODO(MLB): do some buffering?
        self.writer.write_from(bytes.as_slice()).await?;

        Ok(())
    }

    /// Finishes writing, flushing all remaining bytes to the file.
    #[inline]
    pub(crate) async fn finish(mut self) -> Result<()> {
        self.writer.close().await?;

        Ok(())
    }
}

impl Display for Kind {
    #[inline]
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            Self::Delta => write!(f, "delta"),
            Self::Snapshot => write!(f, "snapshot"),
        }
    }
}
