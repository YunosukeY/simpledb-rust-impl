#![allow(dead_code)]

use crate::util::Result;

use super::constant::Constant;

pub trait Scan {
    fn before_first(&mut self) -> Result<()>;

    fn next(&mut self) -> Result<bool>;

    fn get_int(&mut self, field_name: &str) -> Result<i32>;

    fn get_double(&mut self, field_name: &str) -> Result<f64>;

    fn get_bytes(&mut self, field_name: &str) -> Result<Vec<u8>>;

    fn get_string(&mut self, field_name: &str) -> Result<String>;

    fn get_boolean(&mut self, field_name: &str) -> Result<bool>;

    fn get_date(&mut self, field_name: &str) -> Result<chrono::NaiveDate>;

    fn get_time(&mut self, field_name: &str) -> Result<chrono::NaiveTime>;

    fn get_datetime(&mut self, field_name: &str) -> Result<chrono::DateTime<chrono::FixedOffset>>;

    fn get_json(&mut self, field_name: &str) -> Result<serde_json::Value>;

    fn get_value(&mut self, field_name: &str) -> Result<Constant>;

    fn is_null(&mut self, field_name: &str) -> Result<bool>;

    fn has_field(&self, field_name: &str) -> bool;

    fn close(&self);
}
