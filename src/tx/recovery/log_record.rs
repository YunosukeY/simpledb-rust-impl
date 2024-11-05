#![allow(dead_code)]

use crate::{file::page::Page, tx::transaction::Transaction};

use super::{
    checkpoint_record::CheckpointRecord, commit_record::CommitRecord,
    rollback_record::RollbackRecord, set_bool_record::SetBoolRecord,
    set_bytes_record::SetBytesRecord, set_date_record::SetDateRecord,
    set_datetime_record::SetDatetimeRecord, set_double_record::SetDoubleRecord,
    set_int_record::SetIntRecord, set_json_record::SetJsonRecord,
    set_string_record::SetStringRecord, set_time_record::SetTimeRecord, start_record::StartRecord,
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
pub const SET_DATE: i32 = 9;
pub const SET_TIME: i32 = 10;
pub const SET_DATETIME: i32 = 11;
pub const SET_JSON: i32 = 12;

pub trait LogRecord {
    fn op(&self) -> i32;

    fn tx_num(&self) -> i32;

    fn undo(&self, tx: &mut Transaction);
}

pub fn create_log_record(bytes: Vec<u8>) -> Option<Box<dyn LogRecord>> {
    let p = Page::from_bytes(&bytes);
    match p.get_int(0) {
        CHECKPOINT => Some(Box::new(CheckpointRecord::new())),
        START => Some(Box::new(StartRecord::from_page(p))),
        COMMIT => Some(Box::new(CommitRecord::from_page(p))),
        ROLLBACK => Some(Box::new(RollbackRecord::from_page(p))),
        SET_INT => Some(Box::new(SetIntRecord::from_page(p))),
        SET_BYTES => Some(Box::new(SetBytesRecord::from_page(p))),
        SET_STRING => Some(Box::new(SetStringRecord::from_page(p))),
        SET_BOOL => Some(Box::new(SetBoolRecord::from_page(p))),
        SET_DOUBLE => Some(Box::new(SetDoubleRecord::from_page(p))),
        SET_DATE => Some(Box::new(SetDateRecord::from_page(p))),
        SET_TIME => Some(Box::new(SetTimeRecord::from_page(p))),
        SET_DATETIME => Some(Box::new(SetDatetimeRecord::from_page(p))),
        SET_JSON => Some(Box::new(SetJsonRecord::from_page(p))),
        _ => None,
    }
}
