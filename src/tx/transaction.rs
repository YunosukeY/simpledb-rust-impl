#![allow(dead_code)]
#![allow(unused_variables)]

use crate::file::block_id::BlockId;

pub struct Transaction {}

impl Transaction {
    pub fn pin(&mut self, block: &BlockId) {}

    pub fn unpin(&mut self, block: &BlockId) {}

    pub fn set_int(&mut self, block: &BlockId, offset: i32, value: i32, log: bool) {}

    pub fn set_string(&mut self, block: &BlockId, offset: i32, value: &str, log: bool) {}
}
