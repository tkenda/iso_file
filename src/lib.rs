use std::collections::BTreeMap;
use std::mem::transmute;
use std::path::{Path, PathBuf};

use async_recursion::async_recursion;
use header::{IsoDirectoryEntry, IsoDirectoryRecord, IsoFileId, IsoHeader, IsoHeaderRaw};
use tokio::io::{self, AsyncRead, AsyncReadExt, AsyncSeekExt, AsyncWrite, AsyncWriteExt, SeekFrom};

pub mod error;
mod header;

pub use error::{IsoFileError, Result};

#[cfg(test)]
mod test;

/* READ */

#[derive(Debug)]
pub struct IsoFileReader<R>
where
    R: AsyncRead + AsyncSeekExt + Unpin,
{
    header: IsoHeaderRaw,
    entries: BTreeMap<PathBuf, IsoDirectoryEntry>,
    reader: R,
}

impl<R> IsoFileReader<R>
where
    R: AsyncRead + AsyncSeekExt + Unpin,
{
    #[async_recursion(?Send)]
    async fn read_entries(
        reader: &mut R,
        entries: &mut BTreeMap<PathBuf, IsoDirectoryEntry>,
        base: &Path,
        mut offset: u64,
    ) -> io::Result<()> {
        loop {
            reader.seek(SeekFrom::Start(offset)).await?;

            let mut record_buffer = [0u8; size_of::<IsoDirectoryRecord>()];
            reader.read_exact(&mut record_buffer).await?;
            let record: IsoDirectoryRecord = unsafe { transmute(record_buffer) };

            if record.is_empty() {
                break;
            }

            let mut file_id_buffer = vec![0u8; record.file_identifier_length()];
            reader.read_exact(&mut file_id_buffer).await?;

            offset += record.length();

            let file_id = IsoFileId::from(file_id_buffer);

            match file_id {
                IsoFileId::CurrentDirectory => {
                    _ = entries.insert(base.join("."), IsoDirectoryEntry { file_id, record })
                }
                IsoFileId::ParentDirectory => {
                    _ = entries.insert(base.join(".."), IsoDirectoryEntry { file_id, record })
                }
                IsoFileId::File(ref t) => {
                    _ = entries.insert(base.join(t), IsoDirectoryEntry { file_id, record })
                }
                IsoFileId::Directory(ref t) => {
                    if file_id.is_directory() {
                        Self::read_entries(reader, entries, &base.join(t), record.location())
                            .await?;
                    }
                }
            }
        }

        Ok(())
    }

    pub async fn read(mut reader: R) -> io::Result<Self> {
        reader.seek(SeekFrom::Start(0x8000)).await?;

        let mut header_buffer = [0u8; size_of::<IsoHeaderRaw>()];

        reader.read_exact(&mut header_buffer).await?;
        let header: IsoHeaderRaw = unsafe { transmute(header_buffer) };

        let mut entries = BTreeMap::new();
        let base = Path::new("/");

        Self::read_entries(
            &mut reader,
            &mut entries,
            base,
            header.root_entry_location(),
        )
        .await?;

        Ok(Self {
            header,
            entries,
            reader,
        })
    }

    pub fn header(&self) -> IsoHeader {
        self.header.as_ref().into()
    }

    pub fn entries(&self) -> &BTreeMap<PathBuf, IsoDirectoryEntry> {
        &self.entries
    }

    pub async fn read_file<P: Into<PathBuf> + Ord>(&mut self, entry: P) -> Result<Vec<u8>> {
        match self.entries.get(&entry.into()) {
            Some(value) => match &value.file_id {
                IsoFileId::CurrentDirectory => Err(IsoFileError::EntryCurrentDirectory),
                IsoFileId::ParentDirectory => Err(IsoFileError::EntryParentDirectory),
                IsoFileId::Directory(_) => Err(IsoFileError::EntryDirectory),
                IsoFileId::File(_) => {
                    self.reader
                        .seek(SeekFrom::Start(value.record.location()))
                        .await?;

                    let mut buffer = vec![0u8; value.record.length() as usize];
                    self.reader.read_exact(&mut buffer).await?;

                    Ok(buffer)
                }
            },
            None => Err(IsoFileError::FileNotFound),
        }
    }
}

/* WRITE */

pub struct IsoFileWriter<W>
where
    W: AsyncWrite + Unpin,
{
    writer: W,
}

impl<W> IsoFileWriter<W>
where
    W: AsyncWrite + Unpin,
{
    pub async fn new(writer: W) -> io::Result<Self> {
        let mut value = Self { writer };

        value.writer.write_all(&[0u8; 0x8000]).await?;

        Ok(value)
    }

    pub async fn append(&mut self, path: &str, content: &[u8]) -> io::Result<()> {
        let path_bytes = path.as_bytes();

        Ok(())
    }

    pub async fn close(&mut self) -> io::Result<()> {
        self.writer.flush().await?;
        Ok(())
    }
}
