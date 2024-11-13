#![allow(dead_code)]

use crate::record::rid::Rid;

use super::{constant::Constant, scan::Scan};

pub trait UpdateScan: Scan {
    fn set_value(&mut self, field_name: &str, value: Constant);

    fn set_int(&mut self, field_name: &str, value: i32);

    fn set_double(&mut self, field_name: &str, value: f64);

    fn set_bytes(&mut self, field_name: &str, value: &[u8]);

    fn set_string(&mut self, field_name: &str, value: &str);

    fn set_boolean(&mut self, field_name: &str, value: bool);

    fn set_date(&mut self, field_name: &str, value: chrono::NaiveDate);

    fn set_time(&mut self, field_name: &str, value: chrono::NaiveTime);

    fn set_datetime(&mut self, field_name: &str, value: chrono::DateTime<chrono::FixedOffset>);

    fn set_json(&mut self, field_name: &str, value: &serde_json::Value);

    fn insert(&mut self);

    fn delete(&mut self);

    fn get_rid(&self) -> Rid;

    fn move_to_rid(&mut self, rid: Rid);
}
