use std::collections::BTreeMap;
use std::mem::transmute;
use std::path::{Path, PathBuf};
use std::{mem, slice};

use async_recursion::async_recursion;
use chrono::{DateTime, Utc};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeekExt, AsyncWriteExt, SeekFrom};

use crate::Result;
use crate::types::DecDateTime;
use crate::types::IsoDateTime;
use crate::types::LsbMsb;

pub const LOGICAL_BLOCK_SIZE: usize = 2048;

macro_rules! utf8_trimmed {
    ($field:expr) => {
        std::str::from_utf8($field)
            .ok()
            .map(|t| t.trim())
            .filter(|t| !t.is_empty())
            .map(String::from)
    };
}

macro_rules! a_characters {
    ($field:expr, $size:expr) => {
        $field
            .as_deref()
            .map(|t| {
                let filtered = t.chars().filter(|&c| matches!(c,
                    'A'..='Z' | '0'..='9' | '_' |
                    '!' | '"' | '%' | '&' | '\'' | '(' | ')' | '*' | '+' | ',' | '-' | '.' | '/' |
                    ':' | ';' | '<' | '=' | '>' | '?')).collect::<String>();

                let mut array_tmp = [0x20u8; $size];
                let len = filtered.len().min($size);
                array_tmp[..len].copy_from_slice(&filtered.as_bytes()[..len]);
                array_tmp
            })
            .unwrap_or([0x20u8; $size])
    };
}

macro_rules! d_characters {
    ($field:expr, $size:expr) => {
        $field
            .as_deref()
            .map(|t| {
                let filtered = t.chars().filter(|&c| matches!(c, 'A'..='Z' | '0'..='9' | '_')).collect::<String>();

                let mut array_tmp = [0x20u8; $size];
                let len = filtered.len().min($size);
                array_tmp[..len].copy_from_slice(&filtered.as_bytes()[..len]);
                array_tmp
            })
            .unwrap_or([0x20u8; $size])
    };
}

#[derive(Debug, Clone, Copy, Default)]
#[repr(C, packed(1))]
pub(crate) struct RootDirectoryEntryRaw {
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

#[derive(Debug)]

pub(crate) struct RootDirectoryEntry {
    pub location_of_extent: usize,
    pub data_length: usize,
    pub datetime: DateTime<Utc>,
}

impl RootDirectoryEntry {
    pub(crate) fn into_raw(self) -> Result<RootDirectoryEntryRaw> {
        Ok(RootDirectoryEntryRaw {
            length: 34,
            extended_attribute_length: 0,
            location_of_extent: LsbMsb::new_u32(self.location_of_extent as u32),
            data_length: LsbMsb::new_u32(self.data_length as u32),
            datetime: (&self.datetime).try_into()?,
            flags: 2,
            unit_size: 0,
            interleave_gap_size: 0,
            volume_seq_number: LsbMsb::new_u16(1),
            file_identifier_length: 1,
            file_identifier: [0],
        })
    }
}

#[derive(Debug, Clone, Copy)]
#[repr(C, packed(1))]
pub(crate) struct IsoHeaderRaw {
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

    pub async fn read<R: AsyncReadExt + Unpin>(reader: &mut R) -> Result<Self> {
        let mut header_buffer = [0u8; size_of::<Self>()];

        reader.read_exact(&mut header_buffer).await?;
        let header: Self = unsafe { transmute(header_buffer) };

        Ok(header)
    }

    pub async fn write<W: AsyncWriteExt + Unpin>(&self, writer: &mut W) -> Result<()> {
        let size = mem::size_of::<Self>();
        let ptr = self as *const Self as *const u8;
        let byte_slice: &[u8] = unsafe { slice::from_raw_parts(ptr, size) };

        writer.write_all(byte_slice).await?;

        Ok(())
    }

    pub fn terminator() -> Self {
        Self {
            type_code: 0xff,
            standard_id: [b'C', b'D', b'0', b'0', b'1'],
            version: 0x01,
            volume_creation_date: DecDateTime::zeroed(),
            volume_modification_date: DecDateTime::zeroed(),
            volume_expiration_date: DecDateTime::zeroed(),
            volume_effective_date: DecDateTime::zeroed(),
            file_structure_version: 0,
            application_used: [0; 512],
            ..Default::default()
        }
    }
}

impl Default for IsoHeaderRaw {
    fn default() -> Self {
        Self {
            // always 0x01 for a primary volume descriptor.
            type_code: 0x01,
            // always 'CD001'
            standard_id: [b'C', b'D', b'0', b'0', b'1'],
            // always 0x01
            version: 0x01,
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
            // the directory records and path table version (always 0x01).
            file_structure_version: 1,
            // always 0x00
            unused03: 0,
            application_used: [0x20; 512],
            reserved: [0; 653],
        }
    }
}

/// Wrapper around ISOHeaderRaw that provides human-readable string data
#[derive(Debug, Clone)]
pub struct IsoHeader {
    pub(crate) system_id: Option<String>,
    pub(crate) volumen_id: Option<String>,
    pub(crate) volume_space_size: u32,
    pub(crate) volume_set_size: u16,
    pub(crate) volume_sequence_number: u16,
    pub(crate) logical_block_size: u16,
    pub(crate) path_table_size: u32,
    pub(crate) loc_of_type_l_path_table: u32,
    pub(crate) loc_of_opti_l_path_table: u32,
    pub(crate) loc_of_type_m_path_table: u32,
    pub(crate) loc_of_opti_m_path_table: u32,
    pub(crate) volume_set_id: Option<String>,
    pub(crate) publisher_id: Option<String>,
    pub(crate) data_preparer_id: Option<String>,
    pub(crate) application_id: Option<String>,
    pub(crate) copyright_file_id: Option<String>,
    pub(crate) abstract_file_id: Option<String>,
    pub(crate) bibliographic_file_id: Option<String>,
    pub(crate) volume_creation_date: Option<DateTime<Utc>>,
    pub(crate) volume_modification_date: Option<DateTime<Utc>>,
    pub(crate) volume_expiration_date: Option<DateTime<Utc>>,
    pub(crate) volume_effective_date: Option<DateTime<Utc>>,
}

impl IsoHeader {
    pub fn set_system_id<P: Into<String>>(&mut self, system_id: P) {
        self.system_id = Some(system_id.into());
    }

    pub fn set_volumen_id<P: Into<String>>(&mut self, volumen_id: P) {
        self.volumen_id = Some(volumen_id.into());
    }

    pub fn set_volume_set_id<P: Into<String>>(&mut self, volume_set_id: P) {
        self.volume_set_id = Some(volume_set_id.into());
    }

    pub fn set_publisher_id<P: Into<String>>(&mut self, publisher_id: P) {
        self.publisher_id = Some(publisher_id.into());
    }

    pub fn set_data_preparer_id<P: Into<String>>(&mut self, data_preparer_id: P) {
        self.data_preparer_id = Some(data_preparer_id.into());
    }

    pub fn set_application_id<P: Into<String>>(&mut self, application_id: P) {
        self.application_id = Some(application_id.into());
    }

    pub fn set_copyright_file_id<P: Into<String>>(&mut self, copyright_file_id: P) {
        self.copyright_file_id = Some(copyright_file_id.into());
    }

    pub fn set_abstract_file_id<P: Into<String>>(&mut self, abstract_file_id: P) {
        self.abstract_file_id = Some(abstract_file_id.into());
    }

    pub fn set_bibliographic_file_id<P: Into<String>>(&mut self, bibliographic_file_id: P) {
        self.bibliographic_file_id = Some(bibliographic_file_id);
    }

    pub fn set_volume_creation_date(&mut self, volume_creation_date: DateTime<Utc>) {
        self.volume_creation_date = Some(volume_creation_date);
    }

    pub fn set_volume_modification_date(&mut self, volume_modification_date: DateTime<Utc>) {
        self.volume_modification_date = Some(volume_modification_date);
    }

    pub fn set_volume_expiration_date(&mut self, volume_expiration_date: DateTime<Utc>) {
        self.volume_expiration_date = Some(volume_expiration_date);
    }

    pub fn set_volume_effective_date(&mut self, volume_effective_date: DateTime<Utc>) {
        self.volume_effective_date = Some(volume_effective_date);
    }

    pub(crate) fn into_raw(self, root_directory: RootDirectoryEntry) -> Result<IsoHeaderRaw> {
        Ok(IsoHeaderRaw {
            system_id: a_characters!(self.system_id, 32),
            volumen_id: d_characters!(self.volumen_id, 32),
            volume_space_size: LsbMsb::new_u32(self.volume_space_size),
            volume_set_size: LsbMsb::new_u16(self.volume_set_size),
            volume_sequence_number: LsbMsb::new_u16(self.volume_sequence_number),
            logical_block_size: LsbMsb::new_u16(self.logical_block_size),
            path_table_size: LsbMsb::new_u32(self.path_table_size),
            loc_of_type_l_path_table: self.loc_of_type_l_path_table,
            loc_of_opti_l_path_table: self.loc_of_opti_l_path_table,
            loc_of_type_m_path_table: self.loc_of_type_m_path_table.to_be(),
            loc_of_opti_m_path_table: self.loc_of_opti_m_path_table.to_be(),
            root_directory_entry: root_directory.into_raw()?,
            volume_set_id: d_characters!(self.volume_set_id, 128),
            publisher_id: a_characters!(self.publisher_id, 128),
            data_preparer_id: a_characters!(self.data_preparer_id, 128),
            application_id: a_characters!(self.application_id, 128),
            copyright_file_id: d_characters!(self.copyright_file_id, 37),
            abstract_file_id: d_characters!(self.abstract_file_id, 37),
            bibliographic_file_id: d_characters!(self.bibliographic_file_id, 37),
            volume_creation_date: self.volume_creation_date.try_into()?,
            volume_modification_date: self.volume_modification_date.try_into()?,
            volume_expiration_date: self.volume_expiration_date.try_into()?,
            volume_effective_date: self.volume_effective_date.try_into()?,
            ..Default::default()
        })
    }
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
            logical_block_size: LOGICAL_BLOCK_SIZE as u16,
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
            volume_creation_date: Some(Utc::now()),
            volume_modification_date: Some(Utc::now()),
            volume_expiration_date: None,
            volume_effective_date: Some(Utc::now()),
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

    pub fn length(&self) -> u32 {
        self.length as u32
    }

    pub fn is_empty(&self) -> bool {
        self.length == 0
    }

    pub fn data_length(&self) -> u32 {
        self.data_length.lsb()
    }

    pub fn set_data_length(&mut self, length: usize) {
        self.data_length = LsbMsb::new_u32(length as u32);
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

    pub fn set_location(&mut self, location: usize) {
        self.location_of_extent = LsbMsb::new_u32(location as u32);
    }
}

#[derive(Debug, Clone)]
pub struct IsoDirectoryEntry {
    entry: IsoEntry,
    record: IsoDirectoryHeader,
    is_odd: bool,
}

impl IsoDirectoryEntry {
    pub(crate) fn new(
        data_offset: usize,
        data_length: usize,
        timestamp: &DateTime<Utc>,
        entry: IsoEntry,
    ) -> Self {
        let name = entry.name();
        let name_bytes = name.as_bytes();
        let id_len = name_bytes.len();

        let real_length = 33 + id_len as u8;
        let length = (real_length + 1) & !1;

        let flags = if entry.is_file() { 0 } else { 2 };

        Self {
            entry,
            record: IsoDirectoryHeader {
                length,
                extended_attribute_length: 0,
                location_of_extent: LsbMsb::new_u32(data_offset as u32),
                data_length: LsbMsb::new_u32(data_length as u32),
                datetime: timestamp.try_into().expect("invalid date conversion"),
                flags,
                unit_size: 0,
                interleave_gap_size: 0,
                volume_seq_number: LsbMsb::new_u16(256),
                file_identifier_length: id_len as u8,
            },
            is_odd: real_length != length,
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.record.length as usize
    }

    pub(crate) async fn write<W: AsyncWriteExt + Unpin>(&self, writer: &mut W) -> Result<usize> {
        let name = self.entry.name();
        let name_bytes = name.as_bytes();

        let size = mem::size_of::<IsoDirectoryHeader>();
        let ptr = &self.record as *const IsoDirectoryHeader as *const u8;
        let byte_slice: &[u8] = unsafe { slice::from_raw_parts(ptr, size) };

        writer.write_all(byte_slice).await?;
        writer.write_all(name_bytes).await?;

        let odd_size = if self.is_odd {
            writer.write_all(&[0]).await?;
            1
        } else {
            0
        };

        Ok(byte_slice.len() + name_bytes.len() + odd_size)
    }

    pub fn entry(&self) -> &IsoEntry {
        &self.entry
    }

    pub fn record(&self) -> &IsoDirectoryHeader {
        &self.record
    }

    pub fn record_mut(&mut self) -> &mut IsoDirectoryHeader {
        &mut self.record
    }
}

#[derive(Debug, Default)]
pub struct IsoDirectoryEntries(BTreeMap<PathBuf, IsoDirectoryEntry>);

impl IsoDirectoryEntries {
    #[async_recursion(?Send)]
    #[allow(clippy::multiple_bound_locations)]
    pub(crate) async fn read<R: AsyncRead + AsyncSeekExt + Unpin>(
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

            let entry = IsoEntry::from(file_id_buffer);

            let is_odd = record.file_identifier_length() % 2 != 0;

            match entry {
                IsoEntry::CurrentDirectory => {
                    _ = self.0.insert(
                        base.join("."),
                        IsoDirectoryEntry {
                            entry,
                            record,
                            is_odd,
                        },
                    )
                }
                IsoEntry::ParentDirectory => {
                    _ = self.0.insert(
                        base.join(".."),
                        IsoDirectoryEntry {
                            entry,
                            record,
                            is_odd,
                        },
                    )
                }
                IsoEntry::File(ref t) => {
                    _ = self.0.insert(
                        base.join(t),
                        IsoDirectoryEntry {
                            entry,
                            record,
                            is_odd,
                        },
                    )
                }
                IsoEntry::Directory(ref t) => {
                    if entry.is_directory() {
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

    pub fn name(&self) -> String {
        match self {
            IsoEntry::CurrentDirectory => "\0".to_string(),
            IsoEntry::ParentDirectory => "\u{1}".to_string(),
            IsoEntry::Directory(t) => t.to_string(),
            IsoEntry::File(t) => {
                format!("{};1", t)
            }
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

#[derive(Debug, Default, Clone)]
#[repr(C, packed(1))]
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

impl IsoPathTableEntry {
    pub fn new<S: Into<String>>(location: usize, parent_directory: usize, directory_id: S) -> Self {
        let directory_id = directory_id.into();

        let header = IsoPathTableEntryHeader {
            length: directory_id.len() as u8,
            extended_attribute_length: 0,
            location_of_extent: location as u32,
            directory_number_of_parent_directory: parent_directory as u16,
        };

        Self {
            header,
            directory_id,
        }
    }
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

    pub fn as_vec(&self) -> Vec<u8> {
        let mut bytes = Vec::new();

        let entries = match self {
            Self::LTable(t) => t,
            Self::MTable(t) => t,
        };

        for entry in entries {
            let size = mem::size_of::<IsoPathTableEntryHeader>();
            let ptr = &entry.header as *const IsoPathTableEntryHeader as *const u8;
            let byte_slice: &[u8] = unsafe { slice::from_raw_parts(ptr, size) };

            bytes.extend_from_slice(byte_slice);
            bytes.extend_from_slice(entry.directory_id.as_bytes());

            // add one if length is odd
            if entry.header.length & 1 != 0 {
                bytes.push(0x0);
            }
        }

        bytes
    }

    pub fn new_l_table(source: &[Vec<(String, usize)>]) -> Self {
        let mut index = 1;
        let mut folder_map = Vec::new();

        let mut path_table = vec![IsoPathTableEntry::new(23, 1, "\0".to_string())];

        // First level folders
        for folder in &source[0] {
            index += 1;
            path_table.push(IsoPathTableEntry::new(folder.1, 1, folder.0.clone()));
            folder_map.push((folder.clone(), index));
        }

        // Process subfolders
        for (i, subfolders) in source.iter().skip(1).enumerate() {
            if let Some((_, parent_index)) = folder_map.get(i) {
                for subfolder in subfolders {
                    index += 1;
                    path_table.push(IsoPathTableEntry::new(
                        subfolder.1,
                        *parent_index,
                        subfolder.0.clone(),
                    ));
                }
            }
        }

        Self::LTable(path_table)
    }

    pub fn convert_to_m_table(self) -> Self {
        match self {
            Self::LTable(mut t) => {
                t.iter_mut().for_each(|t| {
                    t.header.location_of_extent = t.header.location_of_extent.to_be();
                    t.header.directory_number_of_parent_directory =
                        t.header.directory_number_of_parent_directory.to_be();
                });

                Self::MTable(t)
            }
            Self::MTable(t) => Self::MTable(t),
        }
    }
}
