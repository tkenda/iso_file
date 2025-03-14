use std::collections::BTreeMap;
use std::mem::transmute;
use std::path::{Path, PathBuf};
use std::{mem, slice};

use async_recursion::async_recursion;
use chrono::{DateTime, Utc};
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeekExt, SeekFrom};

use crate::Result;
use crate::types::DecDateTime;
use crate::types::IsoDateTime;
use crate::types::LsbMsb;

const LOGICAL_BLOCK_SIZE: u16 = 2048;

macro_rules! utf8_trimmed {
    ($field:expr) => {
        std::str::from_utf8($field)
            .ok()
            .map(|t| t.trim())
            .filter(|t| !t.is_empty())
            .map(String::from)
    };
}

#[derive(Debug, Clone, Copy, Default)]
#[repr(C, packed(1))]
struct RootDirectoryEntryRaw {
    length: u8,
    extended_attribute_length: u8,
    location_of_extent: LsbMsb<u32>,
    data_length: LsbMsb<u32>,
    datetime: IsoDateTime,
    flags: u8,
    unit_size: u8,
    interleave_gap_size: u8,
    volume_seq_number: LsbMsb<u16>,
    file_identifier_length: u8,
    file_identifier: [u8; 1],
}

#[derive(Debug, Clone, Copy)]
#[repr(C, packed)]
pub struct IsoHeaderRaw {
    type_code: u8,
    standard_id: [u8; 5],
    version: u8,
    unused00: u8,
    system_id: [u8; 32],
    volumen_id: [u8; 32],
    unused01: [u8; 8],
    volume_space_size: LsbMsb<u32>,
    unused02: [u8; 32],
    volume_set_size: LsbMsb<u16>,
    volume_sequence_number: LsbMsb<u16>,
    logical_block_size: LsbMsb<u16>,
    path_table_size: LsbMsb<u32>,
    loc_of_type_l_path_table: u32,
    loc_of_opti_l_path_table: u32,
    loc_of_type_m_path_table: u32,
    loc_of_opti_m_path_table: u32,
    root_directory_entry: RootDirectoryEntryRaw,
    volume_set_id: [u8; 128],
    publisher_id: [u8; 128],
    data_preparer_id: [u8; 128],
    application_id: [u8; 128],
    copyright_file_id: [u8; 37],
    abstract_file_id: [u8; 37],
    bibliographic_file_id: [u8; 37],
    volume_creation_date: DecDateTime,
    volume_modification_date: DecDateTime,
    volume_expiration_date: DecDateTime,
    volume_effective_date: DecDateTime,
    file_structure_version: i8,
    unused03: i8,
    application_used: [u8; 512],
    reserved: [u8; 653],
}

impl IsoHeaderRaw {
    pub fn root_entry_location(&self) -> u32 {
        self.root_directory_entry.location_of_extent.lsb() * self.logical_block_size.lsb() as u32
    }

    pub fn logical_block_size(&self) -> u16 {
        self.logical_block_size.lsb()
    }

    pub fn loc_of_type_l_path_table(&self) -> u32 {
        self.loc_of_type_l_path_table * self.logical_block_size.lsb() as u32
    }
}

impl Default for IsoHeaderRaw {
    fn default() -> Self {
        Self {
            type_code: 0,
            standard_id: [0; 5],
            version: 0,
            unused00: 0,
            system_id: [0; 32],
            volumen_id: [0; 32],
            unused01: [0; 8],
            volume_space_size: LsbMsb::default(),
            unused02: [0; 32],
            volume_set_size: LsbMsb::default(),
            volume_sequence_number: LsbMsb::default(),
            logical_block_size: LsbMsb::default(),
            path_table_size: LsbMsb::default(),
            loc_of_type_l_path_table: 0,
            loc_of_opti_l_path_table: 0,
            loc_of_type_m_path_table: 0,
            loc_of_opti_m_path_table: 0,
            root_directory_entry: RootDirectoryEntryRaw::default(),
            volume_set_id: [0; 128],
            publisher_id: [0; 128],
            data_preparer_id: [0; 128],
            application_id: [0; 128],
            copyright_file_id: [0; 37],
            abstract_file_id: [0; 37],
            bibliographic_file_id: [0; 37],
            volume_creation_date: DecDateTime::default(),
            volume_modification_date: DecDateTime::default(),
            volume_expiration_date: DecDateTime::default(),
            volume_effective_date: DecDateTime::default(),
            file_structure_version: 0,
            unused03: 0,
            application_used: [0; 512],
            reserved: [0; 653],
        }
    }
}

impl IsoHeaderRaw {
    pub async fn read<R: AsyncReadExt + Unpin>(reader: &mut R) -> Result<Self> {
        let mut header_buffer = [0u8; size_of::<Self>()];

        reader.read_exact(&mut header_buffer).await?;
        let header: Self = unsafe { transmute(header_buffer) };

        Ok(header)
    }

    pub fn terminator() -> Self {
        Self {
            type_code: 0xff,
            standard_id: [b'C', b'D', b'0', b'0', b'1'],
            version: 0x01,
            ..Default::default()
        }
    }
}

/// Wrapper around ISOHeaderRaw that provides human-readable string data
#[derive(Debug)]
pub struct IsoHeader {
    pub system_id: Option<String>,
    pub volumen_id: Option<String>,
    pub volume_space_size: u32,
    pub volume_set_size: u16,
    pub volume_sequence_number: u16,
    pub logical_block_size: u16,
    pub path_table_size: u32,
    pub loc_of_type_l_path_table: u32,
    pub loc_of_opti_l_path_table: u32,
    pub loc_of_type_m_path_table: u32,
    pub loc_of_opti_m_path_table: u32,
    pub volume_set_id: Option<String>,
    pub publisher_id: Option<String>,
    pub data_preparer_id: Option<String>,
    pub application_id: Option<String>,
    pub copyright_file_id: Option<String>,
    pub abstract_file_id: Option<String>,
    pub bibliographic_file_id: Option<String>,
    pub volume_creation_date: Option<DateTime<Utc>>,
    pub volume_modification_date: Option<DateTime<Utc>>,
    pub volume_expiration_date: Option<DateTime<Utc>>,
    pub volume_effective_date: Option<DateTime<Utc>>,
}

impl From<&IsoHeaderRaw> for IsoHeader {
    fn from(raw: &IsoHeaderRaw) -> Self {
        Self {
            system_id: utf8_trimmed!(&raw.system_id),
            volumen_id: utf8_trimmed!(&raw.volumen_id),
            volume_space_size: raw.volume_space_size.lsb(),
            volume_set_size: raw.volume_set_size.lsb(),
            volume_sequence_number: raw.volume_sequence_number.lsb(),
            logical_block_size: raw.logical_block_size.lsb(),
            path_table_size: raw.path_table_size.lsb(),
            loc_of_type_l_path_table: raw.loc_of_type_l_path_table,
            loc_of_opti_l_path_table: raw.loc_of_opti_l_path_table,
            loc_of_type_m_path_table: raw.loc_of_type_m_path_table.to_be(),
            loc_of_opti_m_path_table: raw.loc_of_opti_m_path_table.to_be(),
            volume_set_id: utf8_trimmed!(&raw.volume_set_id),
            publisher_id: utf8_trimmed!(&raw.publisher_id),
            data_preparer_id: utf8_trimmed!(&raw.data_preparer_id),
            application_id: utf8_trimmed!(&raw.application_id),
            copyright_file_id: utf8_trimmed!(&raw.copyright_file_id),
            abstract_file_id: utf8_trimmed!(&raw.abstract_file_id),
            bibliographic_file_id: utf8_trimmed!(&raw.bibliographic_file_id),
            volume_creation_date: raw.volume_creation_date.try_into().ok(),
            volume_modification_date: raw.volume_modification_date.try_into().ok(),
            volume_expiration_date: raw.volume_expiration_date.try_into().ok(),
            volume_effective_date: raw.volume_effective_date.try_into().ok(),
        }
    }
}

impl AsRef<IsoHeaderRaw> for IsoHeaderRaw {
    fn as_ref(&self) -> &IsoHeaderRaw {
        self
    }
}

impl Default for IsoHeader {
    fn default() -> Self {
        Self {
            system_id: Some("LINUX".to_string()),
            volumen_id: Some("CDROM".to_string()),
            volume_space_size: 0,
            volume_set_size: 0,
            volume_sequence_number: 0,
            logical_block_size: LOGICAL_BLOCK_SIZE,
            path_table_size: 0,
            loc_of_type_l_path_table: 0,
            loc_of_opti_l_path_table: 0,
            loc_of_type_m_path_table: 0,
            loc_of_opti_m_path_table: 0,
            volume_set_id: None,
            publisher_id: None,
            data_preparer_id: Some("P".to_string()),
            application_id: Some("PROTEUS".to_string()),
            copyright_file_id: None,
            abstract_file_id: None,
            bibliographic_file_id: None,
            volume_creation_date: None,
            volume_modification_date: None,
            volume_expiration_date: None,
            volume_effective_date: None,
        }
    }
}

#[repr(C, packed(1))]
#[derive(Debug, Default, Clone)]
pub struct IsoDirectoryHeader {
    length: u8,
    extended_attribute_length: u8,
    location_of_extent: LsbMsb<u32>,
    data_length: LsbMsb<u32>,
    datetime: IsoDateTime,
    flags: u8,
    unit_size: u8,
    interleave_gap_size: u8,
    volume_seq_number: LsbMsb<u16>,
    file_identifier_length: u8,
}

impl IsoDirectoryHeader {
    pub async fn read<R: AsyncReadExt + Unpin>(reader: &mut R) -> Result<Self> {
        let mut header_buffer = [0u8; size_of::<IsoDirectoryHeader>()];

        reader.read_exact(&mut header_buffer).await?;
        let header: IsoDirectoryHeader = unsafe { transmute(header_buffer) };

        Ok(header)
    }

    /*
    pub fn new_directory() -> Self {

    }
    */

    pub async fn write<W: AsyncWrite + Unpin>(
        writer: &mut W,
        data_offset: u32,
        data_length: u32,
        timestamp: &DateTime<Utc>,
        iso_file_id: IsoEntry,
    ) -> Result<()> {
        let name = match iso_file_id {
            IsoEntry::CurrentDirectory => ".".to_string(),
            IsoEntry::ParentDirectory => "..".to_string(),
            IsoEntry::Directory(t) => t,
            IsoEntry::File(t) => {
                format!("{};1", t)
            }
        };

        let name_bytes = name.as_bytes();
        let id_len = name_bytes.len();

        let real_length = 33 + id_len as u8;
        let length = (real_length + 1) & !1;
        let is_odd = real_length != length;

        let value = Self {
            length,
            extended_attribute_length: 0,
            location_of_extent: LsbMsb::new_u32(data_offset),
            data_length: LsbMsb::new_u32(data_length),
            datetime: timestamp.try_into().expect("invalid date conversion"),
            flags: 0,
            unit_size: 1,
            interleave_gap_size: 0,
            volume_seq_number: LsbMsb::new_u16(256),
            file_identifier_length: id_len as u8,
        };

        let size = mem::size_of::<Self>();
        let ptr = &value as *const Self as *const u8;
        let byte_slice: &[u8] = unsafe { slice::from_raw_parts(ptr, size) };

        writer.write_all(byte_slice).await?;
        writer.write_all(name_bytes).await?;

        if is_odd {
            writer.write_u8(0).await?;
        }

        Ok(())
    }

    pub fn length(&self) -> u32 {
        self.length as u32
    }

    pub fn is_empty(&self) -> bool {
        self.length == 0
    }

    pub fn data_length(&self) -> u32 {
        self.data_length.lsb()
    }

    pub fn file_identifier_length(&self) -> usize {
        self.file_identifier_length as usize
    }

    pub fn location(&self, logical_block_size: Option<u16>) -> u32 {
        match logical_block_size {
            Some(t) => self.location_of_extent.lsb() * t as u32,
            None => self.location_of_extent.lsb(),
        }
    }
}

/// Represents a human-readable directory record.
#[derive(Debug, Clone)]
pub struct IsoDirectoryEntry {
    pub file_id: IsoEntry,
    pub record: IsoDirectoryHeader,
}

impl IsoDirectoryEntry {
    pub fn file_id(&self) -> &IsoEntry {
        &self.file_id
    }

    pub fn record(&self) -> &IsoDirectoryHeader {
        &self.record
    }
}

#[derive(Debug, Default)]
pub struct IsoDirectoryEntries(BTreeMap<PathBuf, IsoDirectoryEntry>);

impl IsoDirectoryEntries {
    #[async_recursion(?Send)]
    #[allow(clippy::multiple_bound_locations)]
    pub async fn read<R: AsyncRead + AsyncSeekExt + Unpin>(
        &mut self,
        reader: &mut R,
        base: &Path,
        logical_block_size: u16,
        mut offset: u32,
    ) -> Result<()> {
        loop {
            reader.seek(SeekFrom::Start(offset.into())).await?;

            let record = IsoDirectoryHeader::read(reader).await?;

            if record.is_empty() {
                break;
            }

            let mut file_id_buffer = vec![0u8; record.file_identifier_length()];
            reader.read_exact(&mut file_id_buffer).await?;

            offset += record.length();

            let file_id = IsoEntry::from(file_id_buffer);

            match file_id {
                IsoEntry::CurrentDirectory => {
                    _ = self
                        .0
                        .insert(base.join("."), IsoDirectoryEntry { file_id, record })
                }
                IsoEntry::ParentDirectory => {
                    _ = self
                        .0
                        .insert(base.join(".."), IsoDirectoryEntry { file_id, record })
                }
                IsoEntry::File(ref t) => {
                    _ = self
                        .0
                        .insert(base.join(t), IsoDirectoryEntry { file_id, record })
                }
                IsoEntry::Directory(ref t) => {
                    if file_id.is_directory() {
                        self.read(
                            reader,
                            &base.join(t),
                            logical_block_size,
                            record.location(Some(logical_block_size)),
                        )
                        .await?;
                    }
                }
            }
        }

        Ok(())
    }

    pub fn get(&self, path: &Path) -> Option<&IsoDirectoryEntry> {
        self.0.get(path)
    }

    pub fn inner(&self) -> &BTreeMap<PathBuf, IsoDirectoryEntry> {
        &self.0
    }
}

/* ISO File ID */

#[derive(Debug, Clone)]
pub enum IsoEntry {
    CurrentDirectory,
    ParentDirectory,
    Directory(String),
    File(String),
}

impl IsoEntry {
    pub fn is_directory(&self) -> bool {
        match self {
            Self::CurrentDirectory => false,
            Self::ParentDirectory => false,
            Self::Directory(_) => true,
            Self::File(_) => false,
        }
    }

    pub fn is_file(&self) -> bool {
        match self {
            Self::CurrentDirectory => false,
            Self::ParentDirectory => false,
            Self::Directory(_) => false,
            Self::File(_) => true,
        }
    }
}

impl From<Vec<u8>> for IsoEntry {
    fn from(src: Vec<u8>) -> Self {
        let str = String::from_utf8_lossy(&src);

        match str.as_ref() {
            "\0" => IsoEntry::CurrentDirectory,
            "\u{1}" => IsoEntry::ParentDirectory,
            _ => {
                if str.contains(";1") {
                    IsoEntry::File(str.strip_suffix(";1").unwrap_or_default().to_string())
                } else {
                    IsoEntry::Directory(str.to_string())
                }
            }
        }
    }
}

/* Path Table */

#[repr(C, packed(1))]
#[derive(Debug, Default, Clone)]
pub struct IsoPathTableEntryHeader {
    length: u8,
    extended_attribute_length: u8,
    location_of_extent: u32,
    directory_number_of_parent_directory: u16,
}

#[derive(Debug, Clone)]
pub struct IsoPathTableEntry {
    header: IsoPathTableEntryHeader,
    directory_id: String,
}

#[derive(Debug, Clone)]
pub enum IsoPathTable {
    LTable(Vec<IsoPathTableEntry>),
    MTable(Vec<IsoPathTableEntry>),
}

impl IsoPathTable {
    pub async fn read_l_table<R: AsyncRead + AsyncSeekExt + Unpin>(
        reader: &mut R,
        location: u32,
    ) -> Result<Self> {
        // go to table location
        reader.seek(SeekFrom::Start(location.into())).await?;

        let mut entries = Vec::new();

        loop {
            let mut header_buffer = [0u8; size_of::<IsoPathTableEntryHeader>()];

            reader.read_exact(&mut header_buffer).await?;
            let header: IsoPathTableEntryHeader = unsafe { transmute(header_buffer) };

            if header.length == 0 {
                break;
            }

            let mut directory_id = vec![0u8; header.length.into()];
            reader.read_exact(&mut directory_id).await?;

            // skip one if length is odd
            if header.length & 1 != 0 {
                let _ = reader.seek(SeekFrom::Current(1)).await?;
            }

            entries.push(IsoPathTableEntry {
                header,
                directory_id: String::from_utf8_lossy(&directory_id).to_string(),
            });
        }

        Ok(Self::LTable(entries))
    }

    pub fn convert_to_m_table(&mut self) {
        match self {
            Self::LTable(t) => {
                for entry in t {
                    entry.header.location_of_extent = entry.header.location_of_extent.to_be();
                }
            }
            Self::MTable(_) => {}
        }
    }
}
