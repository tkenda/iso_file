use std::collections::{BTreeMap, HashMap, HashSet};
use std::io::Cursor;
use std::path::{Path, PathBuf};

use chrono::{DateTime, SecondsFormat, Utc};
use core::{
    IsoDirectoryEntries, IsoDirectoryHeader, IsoEntry, IsoHeader, IsoHeaderRaw, IsoPathTable,
};
use tokio::io::{self, AsyncRead, AsyncReadExt, AsyncSeekExt, AsyncWrite, AsyncWriteExt, SeekFrom};

mod core;
pub mod error;
mod types;

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
    path_table: IsoPathTable,
    entries: IsoDirectoryEntries,
    reader: R,
}

impl<R> IsoFileReader<R>
where
    R: AsyncRead + AsyncSeekExt + Unpin,
{
    pub async fn read(mut reader: R) -> Result<Self> {
        // reserved for boot sector
        reader.seek(SeekFrom::Start(0x8000)).await?;

        // read ISO Header
        let header = IsoHeaderRaw::read(&mut reader).await?;

        // read path table
        let type_l_location = header.loc_of_type_l_path_table();
        let path_table = IsoPathTable::read_l_table(&mut reader, type_l_location).await?;

        // read directory entries
        let base_path = Path::new("/");
        let mut entries = IsoDirectoryEntries::default();

        entries
            .read(
                &mut reader,
                base_path,
                header.logical_block_size(),
                header.root_entry_location(),
            )
            .await?;

        Ok(Self {
            header,
            path_table,
            entries,
            reader,
        })
    }

    pub async fn read_file<P: Into<PathBuf> + Ord>(&mut self, path: P) -> Result<Vec<u8>> {
        match self.entries.get(&path.into()) {
            Some(value) => match &value.file_id {
                IsoEntry::CurrentDirectory => Err(IsoFileError::EntryCurrentDirectory),
                IsoEntry::ParentDirectory => Err(IsoFileError::EntryParentDirectory),
                IsoEntry::Directory(_) => unreachable!(),
                IsoEntry::File(_) => {
                    let logical_block_size = self.header.logical_block_size();

                    self.reader
                        .seek(SeekFrom::Start(
                            value.record.location(Some(logical_block_size)).into(),
                        ))
                        .await?;

                    let mut buffer = vec![0u8; value.record.data_length() as usize];
                    self.reader.read_exact(&mut buffer).await?;

                    Ok(buffer)
                }
            },
            None => Err(IsoFileError::FileNotFound),
        }
    }

    pub fn header(&self) -> IsoHeader {
        self.header.as_ref().into()
    }

    pub fn entries(&self) -> &IsoDirectoryEntries {
        &self.entries
    }

    pub fn path_table(&self) -> &IsoPathTable {
        &self.path_table
    }
}

/* WRITE */

#[derive(Debug, Clone)]
struct FileEntry<'r> {
    path: PathBuf,
    content: Option<&'r [u8]>,
    timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone)]
struct SectorEntry<'r> {
    entry: IsoEntry,
    content: Option<&'r [u8]>,
    timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone)]
struct Sector<'r> {
    location: u32,
    entries: Vec<SectorEntry<'r>>,
}

pub struct IsoFileWriter<'r, W>
where
    W: AsyncWrite + Unpin,
{
    header: IsoHeader,
    files: Vec<FileEntry<'r>>,
    writer: W,
}

fn build_sector(entries: Vec<FileEntry<'_>>, location: u32) -> (Sector<'_>, u32, HashSet<String>) {
    let mut sector_entries = Vec::new();
    let mut folders = HashSet::new();

    sector_entries.push(SectorEntry {
        entry: IsoEntry::CurrentDirectory,
        content: None,
        timestamp: Utc::now(),
    });

    sector_entries.push(SectorEntry {
        entry: IsoEntry::ParentDirectory,
        content: None,
        timestamp: Utc::now(),
    });

    // files
    for entry in entries.iter().filter(|t| t.path.components().count() == 2) {
        let file_name = entry
            .path
            .file_name()
            .unwrap()
            .to_string_lossy()
            .to_string();

        sector_entries.push(SectorEntry {
            entry: IsoEntry::File(file_name),
            content: entry.content,
            timestamp: entry.timestamp,
        })
    }

    // folders
    for entry in entries.iter().filter(|t| t.path.components().count() > 2) {
        let component = entry
            .path
            .components()
            .nth(1)
            .unwrap()
            .as_os_str()
            .to_string_lossy();

        let folder_name = format!("/{}", component);

        if folders.insert(folder_name.clone()) {
            sector_entries.push(SectorEntry {
                entry: IsoEntry::Directory(folder_name),
                content: None,
                timestamp: Utc::now(),
            });
        }
    }

    (
        Sector {
            location,
            entries: sector_entries,
        },
        1,
        folders,
    )
}

fn build_sectors<'r>(
    directory_sectors: &mut Vec<Sector<'r>>,
    directory_location: &mut u32,
    files: &Vec<FileEntry<'r>>,
    base_path_opt: Option<&Path>,
) {
    let base_path = base_path_opt.unwrap_or(Path::new("/"));

    let filtered_entries = files
        .iter()
        .filter_map(|t| {
            if t.path.starts_with(base_path) {
                let stripped = if base_path_opt.is_some() {
                    t.path.strip_prefix(base_path).unwrap()
                } else {
                    &t.path
                };

                Some(FileEntry {
                    path: stripped.to_owned(),
                    content: t.content,
                    timestamp: t.timestamp,
                })
            } else {
                None
            }
        })
        .collect::<Vec<FileEntry<'_>>>();

    let (sector, size, folders) = build_sector(filtered_entries, *directory_location);
    *directory_location += size;

    directory_sectors.push(sector);

    for folder in folders {
        let path = base_path.join(folder);
        build_sectors(directory_sectors, directory_location, files, Some(&path));
    }
}

impl<'r, W> IsoFileWriter<'r, W>
where
    W: AsyncWrite + Unpin,
{
    pub async fn new(writer: W, header: IsoHeader) -> io::Result<Self> {
        Ok(Self {
            writer,
            header,
            files: Vec::new(),
        })
    }

    pub fn append_file<P: Into<PathBuf> + Ord>(
        &mut self,
        path: P,
        content: &'r [u8],
        timestamp: DateTime<Utc>,
    ) {
        self.files.push(FileEntry {
            path: path.into(),
            content: Some(content),
            timestamp,
        });
    }

    pub async fn close(&mut self) -> Result<()> {
        let mut directory_sectors: Vec<Sector> = Vec::new();
        let mut directory_location = 0;

        let mut files_sectors: Vec<Sector> = Vec::new();
        let mut files_location = 0;

        build_sectors(
            &mut directory_sectors,
            &mut directory_location,
            &self.files,
            None,
        );

        println!("{:?}", directory_sectors);

        /*
        // build path table
        // write directory records
        // file data extents
        // terminator descriptor


        let mut buffer = Cursor::new(Vec::new());

        for (path, (content, timestamp)) in sorted_entries {
            let number_of_sectors = (content.len() as u16 / self.header.logical_block_size) + 1;

            println!("{}", number_of_sectors);

            let file_id_name = path.file_name().unwrap().to_string_lossy().to_string();
            let file_id = IsoFileId::File(file_id_name);

            let file_size = content.len() as u32;

            IsoDirectoryHeader::write(&mut buffer, 0, file_size, timestamp, file_id).await?;
        }

        // reserved for boot sector
        // self.writer.write_all(&[0u8; 0x8000]).await?;

        // Root Current Directory
        // IsoDirectoryRecord::write(&mut buffer, 0, 0, &timestamp, IsoFileId::CurrentDirectory).await?;

        self.writer.flush().await?;
        */

        Ok(())
    }
}
