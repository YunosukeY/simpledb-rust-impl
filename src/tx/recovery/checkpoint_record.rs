#![allow(dead_code)]

use crate::{file::page::Page, log::log_manager::LogManager, tx::transaction::Transaction};

use super::log_record::{LogRecord, CHECKPOINT};

pub struct CheckpointRecord {}

impl CheckpointRecord {
    pub fn new() -> CheckpointRecord {
        CheckpointRecord {}
    }

    pub fn write_to_log(lm: &mut LogManager) -> i32 {
        let rec = vec![0; 4];
        let mut p = Page::from_bytes(&rec);
        p.set_int(0, CHECKPOINT);
        lm.append(rec).unwrap()
    }
}

impl LogRecord for CheckpointRecord {
    fn op(&self) -> i32 {
        CHECKPOINT
    }

    fn tx_num(&self) -> i32 {
        -1
    }

    fn undo(&self, _tx: Transaction) {}
}

impl std::fmt::Display for CheckpointRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "<CHECKPOINT>")
    }
}
