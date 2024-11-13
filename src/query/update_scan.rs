#![allow(dead_code)]

use crate::record::rid::RID;

use super::{constant::Constant, scan::Scan};

pub trait UpdateScan: Scan {
    fn set_value(&mut self, field_name: &str, value: Constant);

    fn set_int(&mut self, field_name: &str, value: i32);

    fn set_string(&mut self, field_name: &str, value: &str);

    fn insert(&mut self);

    fn delete(&mut self);

    fn get_rid(&self) -> RID;

    fn move_to_rid(&mut self, rid: RID);
}
