use crate::{
    Error, LinkId, Result,
    storage::{self, Reader, Writer},
};

/// The footer of a delta file, containing information about it.
///
/// This contains both information which is known when starting to write the file,
/// and information which is only known once writing it is done. Since we want to
/// support writing to files which only support being appended to, and we also want
/// to minimize the amount of read requests we have to issue, we store all of this
/// at the end of the file.
///
/// The storage format is as follows:
/// 1. `previous`, encoded as a `u128` in big-endian order, where `0` represents
///    `None`, and the `u128` otherwise represents a UUID.
/// 2. `index`, encoded in big-endian order.
/// 3. `total`, encoded in big-endian order.
/// 4. `count`, encoded in big-endian order.
/// 5. `VERSION`, encoded in big-endian order.
///
/// `VERSION` is stored last to make sure that if we add more fields in later
/// versions of the storage format, the version that was used to encode a file is
/// stored at the same offset from the end of the file, to make sure that we detect
/// any incompabilities when trying to decode a snapshot.
pub struct Footer {
    /// The ID of the previous link which this link extends.
    pub previous: Option<LinkId>,

    /// The index of this link in the chain of links.
    ///
    /// This is equal to the number of previous links in the chain.
    pub index: u32,

    /// The number of entries present in the link's snapshot.
    ///
    /// This is equal to the number of entries present in this link's delta as well as
    /// all of the previous links'.
    pub total: u32,

    /// The number of entries present in the link's delta.
    pub count: u32,
}

impl Footer {
    /// The expected size of the footer of a delta file.
    ///
    /// Future storage formats might have a bigger footer than this value.
    pub const SIZE: usize = 30; // 16 + 3 * 4 + 2

    /// Reads the [`Footer`] supposedly stored at the end of the file being read by
    /// `reader`.
    ///
    /// This updates the `reader` so that it will act as-if the footer did not exist.
    pub async fn read(reader: &mut Reader) -> Result<Self> {
        if reader.file_size() < Self::SIZE {
            return Err(Error::FileSize {
                expected: Self::SIZE,
                got: reader.file_size(),
            });
        }

        reader.goto(-2)?;
        let version = reader.read_u16().await?;

        if version != storage::VERSION {
            return Err(Error::Version {
                expected: storage::VERSION,
                got: version,
            });
        }

        reader.goto(-(Self::SIZE as isize))?;

        let previous = reader.read_u128().await?;
        let previous = if previous == 0 {
            None
        } else {
            Some(LinkId::from_u128(previous))
        };

        let index = reader.read_u32().await?;
        let total = reader.read_u32().await?;
        let count = reader.read_u32().await?;

        let end = reader.file_size() - Self::SIZE;
        reader.set_file_size(end);

        Ok(Self {
            previous,
            index,
            total,
            count,
        })
    }

    /// Writes the [`Footer`] to the writer.
    pub async fn write(&self, writer: &mut Writer) -> Result<()> {
        let Self {
            previous,
            index,
            total,
            count,
        } = self;

        let previous = previous.as_ref().map(LinkId::as_u128).unwrap_or_default();

        writer.write_u128(previous).await?;
        writer.write_u32(*index).await?;
        writer.write_u32(*total).await?;
        writer.write_u32(*count).await?;
        writer.write_u16(storage::VERSION).await?;

        Ok(())
    }
}
