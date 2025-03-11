use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Utc};

use crate::{IsoFileError, Result};

const DISK_SECTOR_SIZE: u64 = 2048;

macro_rules! utf8_trimmed {
    ($field:expr) => {
        std::str::from_utf8($field)
            .unwrap_or_default()
            .trim()
            .to_string()
    };
}

#[derive(Debug, Clone, Copy, Default)]
#[repr(C, packed)]
struct DecDateTime {
    year: u32,
    month: u16,
    day: u16,
    hour: u16,
    minute: u16,
    second: u16,
    milli: u16,
    tz_offset: u8,
}

impl DecDateTime {
    pub fn into_chrono(self) -> Result<DateTime<Utc>> {
        let date = NaiveDate::from_ymd_opt(self.year as i32, self.month as u32, self.day as u32)
            .ok_or(IsoFileError::InvalidDate)?;

        let time = NaiveTime::from_hms_milli_opt(
            self.hour as u32,
            self.minute as u32,
            self.second as u32,
            self.milli as u32,
        )
        .ok_or(IsoFileError::InvalidTime)?;

        let date_time = NaiveDateTime::new(date, time);

        // Time zone offset is stored in 15-minute intervals; convert to seconds.
        let offset_minutes = (self.tz_offset as i32) * 15;
        let fixed_offset = FixedOffset::west_opt(offset_minutes * 60).unwrap();

        // Convert to `DateTime<FixedOffset>` first, then convert to `Utc`.
        let dt_fixed = fixed_offset.from_local_datetime(&date_time).unwrap();
        Ok(dt_fixed.with_timezone(&Utc))
    }
}

#[derive(Debug, Clone, Copy, Default)]
#[repr(C, packed)]
struct RootDirectoryEntryRaw {
    length: u8,
    extended_attribute_length: u8,
    location_of_extent: u32,
    data_length: u32,
    datetime: DecDateTime,
    flags: u8,
    unit_size: u8,
    interleave_gap_size: u8,
    volume_seq_number: u16,
    file_identifier_length: u8,
    file_identifier: [u8; 2],
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
    volume_space_size: [u32; 2],
    unused02: [u8; 32],
    volume_set_size: u32,
    volume_sequence_number: u32,
    logical_block_size: u32,
    path_table_size: [u32; 2],
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
    pub fn root_entry_location(&self) -> u64 {
        self.root_directory_entry.location_of_extent as u64 * DISK_SECTOR_SIZE
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
            volume_space_size: [0; 2],
            unused02: [0; 32],
            volume_set_size: 0,
            volume_sequence_number: 0,
            logical_block_size: 0,
            path_table_size: [0; 2],
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

/// Wrapper around ISOHeaderRaw that provides human-readable string data
#[derive(Debug)]
pub struct IsoHeader {
    pub system_id: String,
    pub volumen_id: String,
    pub volume_set_id: String,
    pub publisher_id: String,
    pub data_preparer_id: String,
    pub application_id: String,
    pub copyright_file_id: String,
    pub abstract_file_id: String,
    pub bibliographic_file_id: String,
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
            volume_set_id: utf8_trimmed!(&raw.volume_set_id),
            publisher_id: utf8_trimmed!(&raw.publisher_id),
            data_preparer_id: utf8_trimmed!(&raw.data_preparer_id),
            application_id: utf8_trimmed!(&raw.application_id),
            copyright_file_id: utf8_trimmed!(&raw.copyright_file_id),
            abstract_file_id: utf8_trimmed!(&raw.abstract_file_id),
            bibliographic_file_id: utf8_trimmed!(&raw.bibliographic_file_id),
            volume_creation_date: raw.volume_creation_date.into_chrono().ok(),
            volume_modification_date: raw.volume_modification_date.into_chrono().ok(),
            volume_expiration_date: raw.volume_expiration_date.into_chrono().ok(),
            volume_effective_date: raw.volume_effective_date.into_chrono().ok(),
        }
    }
}

impl AsRef<IsoHeaderRaw> for IsoHeaderRaw {
    fn as_ref(&self) -> &IsoHeaderRaw {
        self
    }
}

#[repr(C, packed(1))]
#[derive(Debug, Default, Clone)]
pub struct IsoDirectoryRecord {
    length: u8,
    extended_attribute_length: u8,
    location_of_extent: u32,
    data_length: u32,
    datetime: DecDateTime,
    flags: u8,
    unit_size: u8,
    interleave_gap_size: u8,
    volume_seq_number: u16,
    file_identifier_length: u8,
}

impl IsoDirectoryRecord {
    pub fn length(&self) -> u64 {
        self.length as u64
    }

    pub fn is_empty(&self) -> bool {
        self.length == 0
    }

    pub fn file_identifier_length(&self) -> usize {
        self.file_identifier_length as usize
    }

    pub fn location(&self) -> u64 {
        self.location_of_extent as u64 * DISK_SECTOR_SIZE
    }
}

#[derive(Debug, Clone)]
pub enum IsoFileId {
    CurrentDirectory,
    ParentDirectory,
    Directory(String),
    File(String),
}

impl IsoFileId {
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

impl From<Vec<u8>> for IsoFileId {
    fn from(src: Vec<u8>) -> Self {
        let str = String::from_utf8_lossy(&src);

        match str.as_ref() {
            "\0" => IsoFileId::CurrentDirectory,
            "\u{1}" => IsoFileId::ParentDirectory,
            _ => {
                if str.contains(";1") {
                    IsoFileId::File(str.strip_suffix(";1").unwrap_or_default().to_string())
                } else {
                    IsoFileId::Directory(str.to_string())
                }
            }
        }
    }
}

/// Represents a human-readable directory record.
#[derive(Debug, Clone)]
pub struct IsoDirectoryEntry {
    pub file_id: IsoFileId,
    pub record: IsoDirectoryRecord,
}
