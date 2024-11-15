#![allow(dead_code)]

use crate::{file::page::Page, tx::transaction::Transaction, util::Result};

use super::{
    checkpoint_record::CheckpointRecord, commit_record::CommitRecord, nq_ckpt_record::NqCkptRecord,
    rollback_record::RollbackRecord, set_bool_record::SetBoolRecord,
    set_bytes_record::SetBytesRecord, set_date_record::SetDateRecord,
    set_datetime_record::SetDatetimeRecord, set_double_record::SetDoubleRecord,
    set_int_record::SetIntRecord, set_json_record::SetJsonRecord,
    set_string_record::SetStringRecord, set_time_record::SetTimeRecord, start_record::StartRecord,
};

pub const CHECKPOINT: i32 = 0;
pub const NQCKPT: i32 = 13;
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

pub fn create_log_record(bytes: Vec<u8>) -> Result<Box<dyn LogRecord>> {
    let p = Page::from(bytes);
    match p.get_int(0) {
        CHECKPOINT => Ok(Box::new(CheckpointRecord::new())),
        NQCKPT => Ok(Box::new(NqCkptRecord::from(p))),
        START => Ok(Box::new(StartRecord::from(p))),
        COMMIT => Ok(Box::new(CommitRecord::from(p))),
        ROLLBACK => Ok(Box::new(RollbackRecord::from(p))),
        SET_INT => Ok(Box::new(SetIntRecord::from(p))),
        SET_BYTES => Ok(Box::new(SetBytesRecord::from(p))),
        SET_STRING => Ok(Box::new(SetStringRecord::from(p))),
        SET_BOOL => Ok(Box::new(SetBoolRecord::from(p))),
        SET_DOUBLE => Ok(Box::new(SetDoubleRecord::from(p))),
        SET_DATE => Ok(Box::new(SetDateRecord::try_from(p)?)),
        SET_TIME => Ok(Box::new(SetTimeRecord::try_from(p)?)),
        SET_DATETIME => Ok(Box::new(SetDatetimeRecord::from(p))),
        SET_JSON => Ok(Box::new(SetJsonRecord::from(p))),
        _ => Err("invalid log record".into()),
    }
}
