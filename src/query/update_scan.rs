#![allow(dead_code)]

use crate::{record::rid::Rid, util::Result};

use super::{constant::Constant, scan::Scan};

pub trait UpdateScan: Scan {
    fn set_value(&mut self, field_name: &str, value: Constant) -> Result<()>;

    fn set_int(&mut self, field_name: &str, value: i32) -> &mut Self;

    fn set_double(&mut self, field_name: &str, value: f64) -> &mut Self;

    fn set_bytes(&mut self, field_name: &str, value: &[u8]) -> &mut Self;

    fn set_string(&mut self, field_name: &str, value: &str) -> &mut Self;

    fn set_boolean(&mut self, field_name: &str, value: bool) -> &mut Self;

    fn set_date(&mut self, field_name: &str, value: chrono::NaiveDate) -> &mut Self;

    fn set_time(&mut self, field_name: &str, value: chrono::NaiveTime) -> &mut Self;

    fn set_datetime(
        &mut self,
        field_name: &str,
        value: chrono::DateTime<chrono::FixedOffset>,
    ) -> &mut Self;

    fn set_json(&mut self, field_name: &str, value: &serde_json::Value) -> &mut Self;

    fn insert(&mut self) -> Result<()>;

    fn delete(&mut self) -> Result<()>;

    fn get_rid(&self) -> Rid;

    fn move_to_rid(&mut self, rid: Rid) -> Result<()>;
}
