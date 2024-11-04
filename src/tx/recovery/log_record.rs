#![allow(dead_code)]

use crate::{file::page::Page, tx::transaction::Transaction};

use super::{checkpoint_record::CheckpointRecord, commit_record::CommitRecord};

pub const CHECKPOINT: i32 = 0;
pub const START: i32 = 1;
pub const COMMIT: i32 = 2;
pub const ROLLBACK: i32 = 3;
pub const SET_INT: i32 = 4;
pub const SET_STRING: i32 = 5;

pub trait LogRecord {
    fn op(&self) -> i32;

    fn tx_num(&self) -> i32;

    fn undo(&self, tx: Transaction);
}

// TODO
fn create_log_record(bytes: Vec<u8>) -> Option<Box<dyn LogRecord>> {
    let p = Page::from_bytes(&bytes);
    match p.get_int(0) {
        CHECKPOINT => Some(Box::new(CheckpointRecord::new())),
        COMMIT => Some(Box::new(CommitRecord::new(p))),
        _ => None,
    }
}
