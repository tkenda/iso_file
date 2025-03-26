use chrono::{DateTime, Datelike, FixedOffset, NaiveDate, Offset, TimeZone, Timelike, Utc};

use crate::{IsoFileError, Result};

#[repr(C, packed(1))]
#[derive(Copy, Clone, Debug, Default)]
pub(crate) struct LsbMsb<T> {
    lsb: T,
    msb: T,
}

impl<T: Copy> LsbMsb<T> {
    pub fn lsb(&self) -> T {
        self.lsb
    }
}

impl LsbMsb<u16> {
    pub fn new_u16(lsb: u16) -> Self {
        Self {
            lsb,
            msb: lsb.to_be(),
        }
    }
}

impl LsbMsb<u32> {
    pub fn new_u32(lsb: u32) -> Self {
        Self {
            lsb,
            msb: lsb.to_be(),
        }
    }
}

#[derive(Debug, Clone, Copy)]
#[repr(C, packed(1))]
pub(crate) struct DecDateTime {
    year: [u8; 4],
    month: [u8; 2],
    day: [u8; 2],
    hour: [u8; 2],
    minute: [u8; 2],
    second: [u8; 2],
    milli: [u8; 2],
    tz_offset: [u8; 1],
}

impl DecDateTime {
    pub fn zeroed() -> Self {
        Self {
            year: [0x0; 4],
            month: [0x0; 2],
            day: [0x0; 2],
            hour: [0x0; 2],
            minute: [0x0; 2],
            second: [0x0; 2],
            milli: [0x0; 2],
            tz_offset: [0x0],
        }
    }
}

impl Default for DecDateTime {
    fn default() -> Self {
        Self {
            year: [b'0'; 4],
            month: [b'0'; 2],
            day: [b'0'; 2],
            hour: [b'0'; 2],
            minute: [b'0'; 2],
            second: [b'0'; 2],
            milli: [b'0'; 2],
            tz_offset: [0x0],
        }
    }
}

impl TryInto<DateTime<Utc>> for DecDateTime {
    type Error = IsoFileError;

    fn try_into(self) -> Result<DateTime<Utc>> {
        let year_str = String::from_utf8_lossy(&self.year);
        let month_str = String::from_utf8_lossy(&self.month);
        let day_str = String::from_utf8_lossy(&self.day);
        let hour_str = String::from_utf8_lossy(&self.hour);
        let minute_str = String::from_utf8_lossy(&self.minute);
        let second_str = String::from_utf8_lossy(&self.second);
        let milli_str = String::from_utf8_lossy(&self.milli);

        let tz_offset_str = String::from_utf8_lossy(&self.tz_offset);
        let tz_offset = tz_offset_str
            .parse::<i32>()
            .map_err(|_| IsoFileError::InvalidTimezone)?;

        // Convert 15-minute intervals to seconds
        let fixed_offset =
            FixedOffset::west_opt(tz_offset * 15 * 60).ok_or(IsoFileError::InvalidTimezone)?;

        let datetime_str = format!(
            "{}-{}-{}T{}:{}:{}.{}",
            year_str, month_str, day_str, hour_str, minute_str, second_str, milli_str
        );

        // Parse into DateTime<FixedOffset>
        let datetime = DateTime::parse_from_str(&datetime_str, "%Y-%m-%dT%H:%M:%S%.3f")
            .map_err(|_| IsoFileError::InvalidDatetime)?
            .with_timezone(&fixed_offset);

        // Convert to UTC
        Ok(datetime.with_timezone(&Utc))
    }
}

impl TryFrom<&DateTime<Utc>> for DecDateTime {
    type Error = IsoFileError;

    fn try_from(value: &DateTime<Utc>) -> Result<Self> {
        let year = format!("{:04}", value.year()).into_bytes();
        let month = format!("{:02}", value.month()).into_bytes();
        let day = format!("{:02}", value.day()).into_bytes();
        let hour = format!("{:02}", value.hour()).into_bytes();
        let minute = format!("{:02}", value.minute()).into_bytes();
        let second = format!("{:02}", value.second()).into_bytes();
        let milli = format!("{:03}", value.timestamp_subsec_millis()).into_bytes();

        let offset = value.offset().fix().local_minus_utc();
        let tz_offset = (offset / (15 * 60)) as u8; // Convert seconds to 15-minute intervals

        Ok(Self {
            year: [year[0], year[1], year[2], year[3]],
            month: [month[0], month[1]],
            day: [day[0], day[1]],
            hour: [hour[0], hour[1]],
            minute: [minute[0], minute[1]],
            second: [second[0], second[1]],
            milli: [milli[0], milli[1]],
            tz_offset: [tz_offset],
        })
    }
}

impl TryFrom<DateTime<Utc>> for DecDateTime {
    type Error = IsoFileError;

    fn try_from(value: DateTime<Utc>) -> Result<Self> {
        (&value).try_into()
    }
}

impl TryFrom<Option<DateTime<Utc>>> for DecDateTime {
    type Error = IsoFileError;

    fn try_from(value: Option<DateTime<Utc>>) -> Result<Self> {
        match value {
            Some(t) => Ok(t.try_into()?),
            None => Ok(DecDateTime::default()),
        }
    }
}

#[repr(C, packed(1))]
#[derive(Clone, Copy, Debug, Default)]
pub struct IsoDateTime {
    pub year: u8,
    pub month: u8,
    pub day: u8,
    pub hour: u8,
    pub minute: u8,
    pub second: u8,
    pub gmt_offset: u8,
}

impl TryInto<DateTime<Utc>> for IsoDateTime {
    type Error = IsoFileError;

    fn try_into(self) -> Result<DateTime<Utc>> {
        let fixed_offset = FixedOffset::west_opt((self.gmt_offset as i32) * 15 * 60)
            .ok_or(IsoFileError::InvalidTimezone)?;

        let naive_datetime =
            NaiveDate::from_ymd_opt(self.year as i32 + 1900, self.month as u32, self.day as u32)
                .and_then(|date| {
                    date.and_hms_opt(self.hour as u32, self.minute as u32, self.second as u32)
                })
                .ok_or(IsoFileError::InvalidDatetime)?;

        let datetime = fixed_offset
            .from_local_datetime(&naive_datetime)
            .single()
            .ok_or(IsoFileError::InvalidDatetime)?;

        Ok(datetime.with_timezone(&Utc))
    }
}

impl TryFrom<&DateTime<Utc>> for IsoDateTime {
    type Error = IsoFileError;

    fn try_from(value: &DateTime<Utc>) -> std::result::Result<Self, Self::Error> {
        let offset = value.offset().fix().local_minus_utc();
        let gmt_offset = (offset / (15 * 60)) as u8;

        Ok(IsoDateTime {
            year: (value.year() - 1900) as u8,
            month: value.month() as u8,
            day: value.day() as u8,
            hour: value.hour() as u8,
            minute: value.minute() as u8,
            second: value.second() as u8,
            gmt_offset,
        })
    }
}
