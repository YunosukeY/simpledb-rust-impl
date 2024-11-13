#![allow(dead_code)]

use super::constant::Constant;

pub trait Scan {
    fn before_first(&mut self);

    fn next(&mut self) -> bool;

    fn get_int(&mut self, field_name: &str) -> i32;

    fn get_double(&mut self, field_name: &str) -> f64;

    fn get_bytes(&mut self, field_name: &str) -> Vec<u8>;

    fn get_string(&mut self, field_name: &str) -> String;

    fn get_boolean(&mut self, field_name: &str) -> bool;

    fn get_date(&mut self, field_name: &str) -> chrono::NaiveDate;

    fn get_time(&mut self, field_name: &str) -> chrono::NaiveTime;

    fn get_datetime(&mut self, field_name: &str) -> chrono::DateTime<chrono::FixedOffset>;

    fn get_json(&mut self, field_name: &str) -> serde_json::Value;

    fn get_value(&mut self, field_name: &str) -> Constant;

    fn has_field(&self, field_name: &str) -> bool;

    fn close(&self);
}
