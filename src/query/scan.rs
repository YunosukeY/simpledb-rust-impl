#![allow(dead_code)]

use super::constant::Constant;

pub trait Scan {
    fn before_first(&mut self);

    fn next(&mut self) -> bool;

    fn get_int(&mut self, field_name: &str) -> i32;

    fn get_string(&mut self, field_name: &str) -> String;

    fn get_value(&mut self, field_name: &str) -> Constant;

    fn has_field(&self, field_name: &str) -> bool;

    fn close(&self);
}
