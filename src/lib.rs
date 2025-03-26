use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use core::{IsoDirectoryEntries, IsoDirectoryEntry, RootDirectoryEntry};
use core::{IsoEntry, IsoHeader, IsoHeaderRaw, IsoPathTable};
use tokio::io::{self, AsyncRead, AsyncWrite, SeekFrom};
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

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
            Some(value) => match value.entry() {
                IsoEntry::CurrentDirectory => Err(IsoFileError::EntryCurrentDirectory),
                IsoEntry::ParentDirectory => Err(IsoFileError::EntryParentDirectory),
                IsoEntry::Directory(_) => unreachable!(),
                IsoEntry::File(_) => {
                    let logical_block_size = self.header.logical_block_size();

                    self.reader
                        .seek(SeekFrom::Start(
                            value.record().location(Some(logical_block_size)).into(),
                        ))
                        .await?;

                    let mut buffer = vec![0u8; value.record().data_length() as usize];
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
    content: &'r [u8],
    timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy)]
struct SectorProps {
    group_no: usize,
    depth: usize,
}

fn build_dirs<'r>(
    file_entries: Vec<FileEntry<'r>>,
    files_sectors: &mut Vec<&'r [u8]>,
    group_no: usize,
    depth: usize,
) -> (Vec<(Vec<IsoDirectoryEntry>, SectorProps)>, Vec<String>) {
    let mut dirs_sector = Vec::new();
    let mut dirs_sectors = Vec::new();
    let mut folders: Vec<String> = Vec::new();

    let mut dirs_sector_size = 0;

    let cur_dir = IsoDirectoryEntry::new(0, 0, &Utc::now(), IsoEntry::CurrentDirectory);
    dirs_sector_size += cur_dir.len();
    dirs_sector.push(cur_dir);

    let par_dir = IsoDirectoryEntry::new(0, 0, &Utc::now(), IsoEntry::ParentDirectory);
    dirs_sector_size += par_dir.len();
    dirs_sector.push(par_dir);

    // files
    for entry in file_entries
        .iter()
        .filter(|t| t.path.components().count() == 2)
    {
        let file_name = entry
            .path
            .file_name()
            .unwrap()
            .to_string_lossy()
            .to_string();

        let file_dir = IsoDirectoryEntry::new(
            files_sectors.len(),
            entry.content.len(),
            &entry.timestamp,
            IsoEntry::File(file_name),
        );

        dirs_sector_size += file_dir.len();

        if dirs_sector_size > core::LOGICAL_BLOCK_SIZE {
            dirs_sectors.push((dirs_sector, SectorProps { group_no, depth }));
            dirs_sector_size = file_dir.len();
            dirs_sector = vec![file_dir];
        } else {
            dirs_sector.push(file_dir);
        }

        for chunk in entry.content.chunks(core::LOGICAL_BLOCK_SIZE) {
            files_sectors.push(chunk);
        }
    }

    // folders
    for entry in file_entries
        .iter()
        .filter(|t| t.path.components().count() > 2)
    {
        let folder_name = entry
            .path
            .components()
            .nth(1)
            .unwrap()
            .as_os_str()
            .to_string_lossy()
            .to_string();

        if !folders.iter().any(|t| t == &folder_name) {
            folders.push(folder_name.clone());

            let dir_dir =
                IsoDirectoryEntry::new(0, 0, &Utc::now(), IsoEntry::Directory(folder_name));

            dirs_sector_size += dir_dir.len();

            if dirs_sector_size > core::LOGICAL_BLOCK_SIZE {
                dirs_sectors.push((dirs_sector, SectorProps { group_no, depth }));
                dirs_sector_size = dir_dir.len();
                dirs_sector = vec![dir_dir];
            } else {
                dirs_sector.push(dir_dir);
            }
        }
    }

    dirs_sectors.push((dirs_sector, SectorProps { group_no, depth }));

    (dirs_sectors, folders)
}

fn build_sectors<'r>(
    dirs_sectors: &mut Vec<(Vec<IsoDirectoryEntry>, SectorProps)>,
    files_sectors: &mut Vec<&'r [u8]>,
    group_no: &mut usize,
    files: &Vec<FileEntry<'r>>,
    depth: usize,
    base_path_opt: Option<&Path>,
) {
    let base_path = base_path_opt.unwrap_or(Path::new("/"));

    let filtered_entries = files
        .iter()
        .filter_map(|t| {
            if t.path.starts_with(base_path) {
                let stripped = if base_path_opt.is_some() {
                    PathBuf::from("/").join(t.path.strip_prefix(base_path).unwrap())
                } else {
                    t.path.clone()
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

    let (mut new_dirs_sectors, folders) =
        build_dirs(filtered_entries, files_sectors, *group_no, depth);

    dirs_sectors.append(&mut new_dirs_sectors);

    *group_no += 1;

    for folder in folders {
        build_sectors(
            dirs_sectors,
            files_sectors,
            group_no,
            files,
            depth + 1,
            Some(&base_path.join(folder)),
        );
    }
}

#[derive(Clone, Copy)]
struct GroupValues {
    index: usize,
    count: usize,
    depth: usize,
}

impl GroupValues {
    fn new(index: usize, depth: usize) -> Self {
        Self {
            index,
            depth,
            count: 0,
        }
    }
}

struct Groups(Vec<GroupValues>);

impl Groups {
    fn new(items: &[(Vec<IsoDirectoryEntry>, SectorProps)]) -> Self {
        let mut group_info: HashMap<usize, GroupValues> = HashMap::new();

        for (index, &(_, props)) in items.iter().enumerate() {
            group_info
                .entry(props.group_no)
                .or_insert(GroupValues::new(index, props.depth))
                .count += 1;
        }

        let mut value: Vec<GroupValues> = group_info.into_values().collect();
        value.sort_by_key(|t| t.index);

        Self(value)
    }

    fn get(&self, index: usize) -> &GroupValues {
        self.0.get(index).expect("invalid block number")
    }
}

struct ParentDirectoryStack<'r> {
    stack: VecDeque<GroupValues>,
    groups: &'r Groups,
}

impl<'r> ParentDirectoryStack<'r> {
    fn new(groups: &'r Groups) -> Self {
        Self {
            stack: VecDeque::new(),
            groups,
        }
    }

    fn set(&mut self, props: &SectorProps) {
        if self.stack.len() != props.depth + 1 {
            if self.stack.len() < props.depth + 1 {
                self.stack.push_front(*self.groups.get(props.group_no));
            } else {
                self.stack.pop_front();
            }
        }
    }

    fn get(&self) -> &GroupValues {
        if self.stack.len() == 1 {
            self.stack.front().unwrap()
        } else {
            self.stack.get(1).expect("empty depth stack")
        }
    }
}

fn set_locations(
    start_location: usize,
    dirs_sectors: &mut [(Vec<IsoDirectoryEntry>, SectorProps)],
) -> Vec<Vec<(String, usize)>> {
    let dirs_sectors_count = dirs_sectors.len();

    let groups = Groups::new(dirs_sectors);
    let mut parent_stack = ParentDirectoryStack::new(&groups);
    let mut count_stack = [0usize; 128];

    let mut path_groups: Vec<Vec<(String, usize)>> = Vec::new();

    // iterate over a group of sectors
    for (sector, props) in dirs_sectors.iter_mut() {
        let mut path_group = Vec::new();

        parent_stack.set(props);

        for dirs in sector {
            match dirs.entry() {
                IsoEntry::CurrentDirectory => {
                    let group = groups.get(props.group_no);

                    dirs.record_mut().set_location(start_location + group.index);
                    dirs.record_mut()
                        .set_data_length(group.count * core::LOGICAL_BLOCK_SIZE);
                }
                IsoEntry::ParentDirectory => {
                    let group = parent_stack.get();

                    dirs.record_mut().set_location(start_location + group.index);
                    dirs.record_mut()
                        .set_data_length(group.count * core::LOGICAL_BLOCK_SIZE);
                }
                IsoEntry::Directory(name) => {
                    let mut group;

                    loop {
                        count_stack[props.group_no] += 1;
                        group = groups.get(props.group_no + count_stack[props.group_no]);

                        if group.depth == props.depth + 1 {
                            break;
                        }
                    }

                    let location = start_location + group.index;

                    path_group.push((name.clone(), location));

                    dirs.record_mut().set_location(location);
                    dirs.record_mut()
                        .set_data_length(group.count * core::LOGICAL_BLOCK_SIZE);
                }
                IsoEntry::File(_) => {
                    let location =  dirs.record().location(None) as usize;

                    dirs.record_mut().set_location(
                        start_location + dirs_sectors_count + location,
                    );
                }
            }
        }

        path_groups.push(path_group);
    }

    path_groups
}

#[derive(Clone, Debug)]
pub struct IsoFileWriter<'r, W>
where
    W: AsyncWrite + Unpin,
{
    header: IsoHeader,
    files: Vec<FileEntry<'r>>,
    writer: W,
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

    pub fn append_file(&mut self, path: &str, content: &'r [u8], timestamp: DateTime<Utc>) {
        let a_characters = path
            .to_uppercase()
            .chars()
            .filter(|&c| {
                matches!(c,
            'A'..='Z' | '0'..='9' | '_' |
            '!' | '"' | '%' | '&' | '\'' | '(' | ')' | '*' | '+' | ',' | '-' | '.' | '/' |
            ':' | ';' | '<' | '=' | '>' | '?')
            })
            .collect::<String>();

        let mut new_path = PathBuf::new();

        for component in PathBuf::from(a_characters).components() {
            new_path.push(
                component
                    .as_os_str()
                    .to_string_lossy()
                    .chars()
                    .take(222)
                    .collect::<String>(),
            );
        }

        self.files.push(FileEntry {
            path: new_path,
            content,
            timestamp,
        });
    }

    pub async fn close(&mut self) -> Result<()> {
        let mut dirs_sectors: Vec<(Vec<IsoDirectoryEntry>, SectorProps)> = Vec::new();
        let mut files_sectors: Vec<&'r [u8]> = Vec::new();

        let mut group_no = 0;

        build_sectors(
            &mut dirs_sectors,
            &mut files_sectors,
            &mut group_no,
            &self.files,
            0,
            None,
        );

        let path_groups = set_locations(23, &mut dirs_sectors);

        // create path table
        let l_path_table = IsoPathTable::new_l_table(&path_groups);
        let l_path_table_raw = l_path_table.as_vec();
        let l_path_table_len = l_path_table_raw.len();

        /*
        for (i_sector, (entries, _)) in dirs_sectors.iter().enumerate() {
            println!("> [{}]", i_sector + 23);
            for (i_entry, entry) in entries.iter().enumerate() {
                println!(
                    ">> [{}] loc: {} entry: {:?}",
                    i_entry,
                    entry.record.location(None),
                    entry.entry
                );
            }
        }
        */

        // reserved for boot sector
        self.writer.write_all(&[0u8; 0x8000]).await?;

        // save header
        let header = IsoHeader {
            volume_space_size: (22 + dirs_sectors.len() + files_sectors.len()) as u32,
            volume_set_size: 1,
            volume_sequence_number: 1,
            path_table_size: l_path_table_len as u32,
            loc_of_type_l_path_table: 19,
            loc_of_type_m_path_table: 21,
            ..self.header.clone()
        };

        // root directory entry
        let root_sectors = dirs_sectors.iter().filter(|t| t.1.group_no == 0).count();

        let root_directory = RootDirectoryEntry {
            location_of_extent: 23,
            data_length: root_sectors * core::LOGICAL_BLOCK_SIZE,
            datetime: Utc::now(),
        };

        let header_raw = header.into_raw(root_directory)?;
        header_raw.write(&mut self.writer).await?;

        let header_term = IsoHeaderRaw::terminator();
        header_term.write(&mut self.writer).await?;

        self.writer.write_all(&[0u8; 0x800]).await?;

        // save path table
        let m_path_table = l_path_table.convert_to_m_table();
        let m_path_table_raw = m_path_table.as_vec();

        {
            let mut l_path_table_buffer = vec![0u8; core::LOGICAL_BLOCK_SIZE * 2];

            assert!(
                l_path_table_len <= l_path_table_buffer.len(),
                "l path table is too large"
            );

            l_path_table_buffer[..l_path_table_raw.len()].copy_from_slice(&l_path_table_raw);
            self.writer.write_all(&l_path_table_buffer).await?;
        }

        {
            let mut m_path_table_buffer = vec![0u8; core::LOGICAL_BLOCK_SIZE * 2];

            assert!(
                l_path_table_len <= m_path_table_buffer.len(),
                "m path table is too large"
            );

            m_path_table_buffer[..m_path_table_raw.len()].copy_from_slice(&m_path_table_raw);
            self.writer.write_all(&m_path_table_buffer).await?;
        }

        // save dirs sectors
        for (sector, _) in dirs_sectors {
            let mut size = core::LOGICAL_BLOCK_SIZE;

            for entry in sector {
                size -= entry.write(&mut self.writer).await?;
            }

            let zeroed = vec![0u8; size];
            self.writer.write_all(&zeroed).await?;
        }

        // save files sectors
        for sector in files_sectors {
            let mut buffer = vec![0u8; core::LOGICAL_BLOCK_SIZE];
            let len = sector.len().min(core::LOGICAL_BLOCK_SIZE);
            buffer[..len].copy_from_slice(&sector[..len]);
            self.writer.write_all(&buffer).await?;
        }

        self.writer.flush().await?;

        Ok(())
    }
}
