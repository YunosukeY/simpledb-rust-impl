#![allow(dead_code)]
#![allow(deprecated)]

use std::str::from_utf8;

use chrono::{DateTime, Datelike, FixedOffset, NaiveDate, NaiveTime, TimeZone, Timelike};

pub struct Page {
    pub(super) buf: Vec<u8>,
}

impl Page {
    pub fn new(size: i32) -> Page {
        Page {
            buf: vec![0; size as usize],
        }
    }

    pub fn from_bytes(bytes: &[u8]) -> Page {
        Page {
            buf: bytes.to_vec(),
        }
    }

    pub fn buffer(&self) -> &[u8] {
        &self.buf
    }

    pub fn int_len(_value: i32) -> i32 {
        4
    }
    pub fn get_int(&self, offset: i32) -> i32 {
        let ofs = offset as usize;
        let bytes = &self.buf[ofs..ofs + 4];
        let a = bytes.try_into().unwrap();
        i32::from_be_bytes(a)
    }
    pub fn set_int(&mut self, offset: i32, value: i32) {
        let ofs = offset as usize;
        self.buf[ofs..ofs + 4].copy_from_slice(&value.to_be_bytes());
    }

    pub fn bytes_len(bytes: &[u8]) -> i32 {
        bytes.len() as i32 + 4
    }
    pub fn get_bytes(&self, offset: i32) -> &[u8] {
        let len = self.get_int(offset);

        let ofs = offset as usize + 4;
        &self.buf[ofs..ofs + len as usize]
    }
    pub fn set_bytes(&mut self, offset: i32, bytes: &[u8]) {
        let len = bytes.len() as i32;
        self.set_int(offset, len);

        let ofs = offset as usize + 4;
        self.buf[ofs..ofs + bytes.len()].copy_from_slice(bytes);
    }

    pub fn str_len(str: &str) -> i32 {
        Self::bytes_len(str.as_bytes())
    }
    pub fn get_string(&self, offset: i32) -> String {
        let bytes = self.get_bytes(offset);
        from_utf8(bytes).unwrap().to_string()
    }
    pub fn set_string(&mut self, offset: i32, s: &str) {
        let bytes: &[u8] = s.as_bytes();
        self.set_bytes(offset, bytes);
    }

    pub fn bool_len(_value: bool) -> i32 {
        1
    }
    pub fn get_bool(&self, offset: i32) -> bool {
        self.buf[offset as usize] != 0
    }
    pub fn set_bool(&mut self, offset: i32, b: bool) {
        self.buf[offset as usize] = b as u8;
    }

    pub fn double_len(_value: f64) -> i32 {
        8
    }
    pub fn get_double(&self, offset: i32) -> f64 {
        let ofs = offset as usize;
        let bytes = &self.buf[ofs..ofs + 8];
        let a = bytes.try_into().unwrap();
        f64::from_be_bytes(a)
    }
    pub fn set_double(&mut self, offset: i32, value: f64) {
        let ofs = offset as usize;
        self.buf[ofs..ofs + 8].copy_from_slice(&value.to_be_bytes());
    }

    pub fn date_len(_value: &NaiveDate) -> i32 {
        6
    }
    pub fn get_date(&self, offset: i32) -> NaiveDate {
        let ofs = offset as usize;
        let bytes = &self.buf[ofs..ofs + 6];
        let y = i32::from_be_bytes(bytes[0..4].try_into().unwrap());
        let m = bytes[4] as u32;
        let d = bytes[5] as u32;
        NaiveDate::from_ymd(y, m, d)
    }
    pub fn set_date(&mut self, offset: i32, date: &NaiveDate) {
        let ofs = offset as usize;
        let y = date.year().to_be_bytes();
        let m = date.month() as u8;
        let d = date.day() as u8;
        let bytes = &[y[0], y[1], y[2], y[3], m, d];
        self.buf[ofs..ofs + 6].copy_from_slice(bytes);
    }

    pub fn time_len(_value: &NaiveTime) -> i32 {
        7
    }
    pub fn get_time(&self, offset: i32) -> NaiveTime {
        let ofs = offset as usize;
        let bytes = &self.buf[ofs..ofs + 7];
        let h = bytes[0] as u32;
        let m = bytes[1] as u32;
        let s = bytes[2] as u32;
        let f = u32::from_be_bytes(bytes[3..7].try_into().unwrap());
        NaiveTime::from_hms_nano(h, m, s, f)
    }
    pub fn set_time(&mut self, offset: i32, time: &NaiveTime) {
        let ofs = offset as usize;
        let h = time.hour() as u8;
        let m = time.minute() as u8;
        let s = time.second() as u8;
        let f = time.nanosecond().to_be_bytes();
        let bytes = &[h, m, s, f[0], f[1], f[2], f[3]];
        self.buf[ofs..ofs + 7].copy_from_slice(bytes);
    }

    pub fn datetime_len(_value: &DateTime<FixedOffset>) -> i32 {
        15
    }
    pub fn get_datetime(&self, offset: i32) -> DateTime<FixedOffset> {
        let ofs = offset as usize;
        let bytes = &self.buf[ofs..ofs + 15];
        let y = u16::from_be_bytes(bytes[0..2].try_into().unwrap()) as i32;
        let mo = bytes[2] as u32;
        let d = bytes[3] as u32;
        let h = bytes[4] as u32;
        let mi = bytes[5] as u32;
        let s = bytes[6] as u32;
        let f = u32::from_be_bytes(bytes[7..11].try_into().unwrap());
        let tz = i32::from_be_bytes(bytes[11..15].try_into().unwrap());
        FixedOffset::east(tz)
            .ymd(y, mo, d)
            .and_hms_nano(h, mi, s, f)
    }
    pub fn set_datetime(&mut self, offset: i32, datetime: &DateTime<FixedOffset>) {
        let ofs = offset as usize;
        let y = (datetime.year() as u16).to_be_bytes();
        let mo = datetime.month() as u8;
        let d = datetime.day() as u8;
        let h = datetime.hour() as u8;
        let mi = datetime.minute() as u8;
        let s = datetime.second() as u8;
        let f = datetime.nanosecond().to_be_bytes();
        let tz = datetime.offset().local_minus_utc().to_be_bytes();
        let bytes = &[
            y[0], y[1], mo, d, h, mi, s, f[0], f[1], f[2], f[3], tz[0], tz[1], tz[2], tz[3],
        ];
        self.buf[ofs..ofs + 15].copy_from_slice(bytes);
    }

    pub fn json_len(json: &serde_json::Value) -> i32 {
        Self::str_len(&json.to_string())
    }
    pub fn get_json(&self, offset: i32) -> serde_json::Value {
        let s = self.get_string(offset);
        serde_json::from_str(&s).unwrap()
    }
    pub fn set_json(&mut self, offset: i32, json: &serde_json::Value) {
        self.set_string(offset, &json.to_string());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn int() {
        let mut p = Page::new(4);

        let values = [0, 1, -1, i32::MAX, i32::MIN];

        for value in values {
            p.set_int(0, value);
            assert_eq!(p.get_int(0), value, "value: {}", value);
        }
    }

    #[test]
    fn bytes() {
        let mut p = Page::new(10);

        let values: [&[u8]; 2] = [&[], &[1, 2, 3]];

        for value in values {
            p.set_bytes(0, value);
            assert_eq!(p.get_bytes(0), value);
        }
    }

    #[test]
    fn string() {
        let mut p = Page::new(7);

        let values = ["", "abc"];

        for value in values {
            p.set_string(0, value);
            assert_eq!(p.get_string(0), value, "value: {}", value);
        }
    }

    #[test]
    fn bool() {
        let mut p = Page::new(1);

        let values = [false, true];

        for value in values {
            p.set_bool(0, value);
            assert_eq!(p.get_bool(0), value, "value: {}", value);
        }
    }

    #[test]
    fn double() {
        let mut p = Page::new(8);

        let values = [
            std::f64::MAX,
            std::f64::MIN,
            std::f64::MIN_POSITIVE,
            std::f64::INFINITY,
            std::f64::NEG_INFINITY,
            0.0,
            -0.0,
        ];

        for value in values {
            p.set_double(0, value);
            assert_eq!(p.get_double(0), value, "value: {}", value);
        }

        p.set_double(0, std::f64::NAN);
        assert!(p.get_double(0).is_nan());
    }

    #[test]
    fn date() {
        let mut p = Page::new(6);

        let values = [
            NaiveDate::from_ymd(2015, 3, 14),
            NaiveDate::from_ymd(0, 1, 1),
            NaiveDate::from_ymd(-4, 2, 29),
            NaiveDate::MAX,
            NaiveDate::MIN,
        ];

        for value in values {
            p.set_date(0, &value);
            assert_eq!(p.get_date(0), value, "value: {}", value);
        }
    }

    #[test]
    fn time() {
        let mut p = Page::new(7);

        let values = [
            NaiveTime::from_hms(15, 4, 5),
            NaiveTime::MIN,
            NaiveTime::from_hms_nano(23, 59, 59, 999_999_999),
            NaiveTime::from_hms_nano(23, 59, 59, 1_999_999_999),
        ];

        for value in values {
            p.set_time(0, &value);
            assert_eq!(p.get_time(0), value, "value: {}", value);
        }
    }

    #[test]
    fn datetime() {
        let mut p = Page::new(15);

        let values = [
            DateTime::parse_from_rfc3339("2006-01-02T15:04:05Z").unwrap(),
            DateTime::parse_from_rfc3339("2006-01-02T15:04:05-07:00").unwrap(),
            DateTime::parse_from_rfc3339("2006-01-02T15:04:05.999999999Z").unwrap(),
            DateTime::parse_from_rfc3339("0000-01-01T00:00:00.000000000+00:00").unwrap(),
            DateTime::parse_from_rfc3339("9999-12-31T23:59:59.999999999+00:00").unwrap(),
        ];

        for value in values {
            p.set_datetime(0, &value);
            assert_eq!(p.get_datetime(0), value, "value: {}", value);
        }
    }

    #[test]
    fn json() {
        let mut p = Page::new(11);

        let values = [
            serde_json::json!(null),
            serde_json::json!(true),
            serde_json::json!(1),
            serde_json::json!("a"),
            serde_json::json!([1, 2, 3]),
            serde_json::json!({"a": 1}),
        ];

        for value in values {
            p.set_json(0, &value);
            assert_eq!(p.get_json(0), value, "value: {}", value);
        }
    }
}
