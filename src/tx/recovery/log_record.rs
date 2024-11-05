#![allow(dead_code)]

use crate::{file::page::Page, tx::transaction::Transaction};

use super::{
    checkpoint_record::CheckpointRecord, commit_record::CommitRecord,
    rollback_record::RollbackRecord, set_bool_record::SetBoolRecord,
    set_bytes_record::SetBytesRecord, set_double_record::SetDoubleRecord,
    set_int_record::SetIntRecord, set_string_record::SetStringRecord, start_record::StartRecord,
};

pub const CHECKPOINT: i32 = 0;
pub const START: i32 = 1;
pub const COMMIT: i32 = 2;
pub const ROLLBACK: i32 = 3;
pub const SET_INT: i32 = 4;
pub const SET_BYTES: i32 = 5;
pub const SET_STRING: i32 = 6;
pub const SET_BOOL: i32 = 7;
pub const SET_DOUBLE: i32 = 8;

pub trait LogRecord {
    fn op(&self) -> i32;

    fn tx_num(&self) -> i32;

    fn undo(&self, tx: &mut Transaction);
}

pub fn create_log_record(bytes: Vec<u8>) -> Option<Box<dyn LogRecord>> {
    let p = Page::from_bytes(&bytes);
    match p.get_int(0) {
        CHECKPOINT => Some(Box::new(CheckpointRecord::new())),
        START => Some(Box::new(StartRecord::new(p))),
        COMMIT => Some(Box::new(CommitRecord::new(p))),
        ROLLBACK => Some(Box::new(RollbackRecord::new(p))),
        SET_INT => Some(Box::new(SetIntRecord::new(p))),
        SET_BYTES => Some(Box::new(SetBytesRecord::new(p))),
        SET_STRING => Some(Box::new(SetStringRecord::new(p))),
        SET_BOOL => Some(Box::new(SetBoolRecord::new(p))),
        SET_DOUBLE => Some(Box::new(SetDoubleRecord::new(p))),
        _ => None,
    }
}
