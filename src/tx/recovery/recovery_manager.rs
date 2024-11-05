#![allow(dead_code)]

use std::{sync::Arc, vec};

use crate::{
    buffer::{buffer::Buffer, buffer_manager::BufferManager},
    log::log_manager::LogManager,
    tx::{
        recovery::{set_bool_record::SetBoolRecord, set_double_record::SetDoubleRecord},
        transaction::Transaction,
    },
};

use super::{
    checkpoint_record::CheckpointRecord,
    commit_record::CommitRecord,
    log_record::{create_log_record, CHECKPOINT, COMMIT, ROLLBACK, START},
    rollback_record::RollbackRecord,
    set_bytes_record::SetBytesRecord,
    set_int_record::SetIntRecord,
    set_string_record::SetStringRecord,
    start_record::StartRecord,
};

pub struct RecoveryManager {
    lm: LogManager,
    bm: Arc<BufferManager>,
    tx_num: i32,
}

impl RecoveryManager {
    pub fn new(tx_num: i32, mut lm: LogManager, bm: Arc<BufferManager>) -> Self {
        StartRecord::write_to_log(&mut lm, tx_num);
        Self { tx_num, lm, bm }
    }

    pub fn commit(&mut self) {
        let bm = Arc::as_ptr(&self.bm) as *mut BufferManager;
        unsafe {
            (*bm).flush_all(self.tx_num).unwrap();
        }
        let lsn = CommitRecord::write_to_log(&mut self.lm, self.tx_num);
        self.lm.flush(lsn).unwrap();
    }

    pub fn rollback(&mut self, tx: &mut Transaction) {
        self.do_rollback(tx);
        let bm = Arc::as_ptr(&self.bm) as *mut BufferManager;
        unsafe {
            (*bm).flush_all(self.tx_num).unwrap();
        }
        let lsn = RollbackRecord::write_to_log(&mut self.lm, self.tx_num);
        self.lm.flush(lsn).unwrap();
    }

    pub fn recover(&mut self, tx: &mut Transaction) {
        self.do_recover(tx);
        let bm = Arc::as_ptr(&self.bm) as *mut BufferManager;
        unsafe {
            (*bm).flush_all(self.tx_num).unwrap();
        }
        let lsn = CheckpointRecord::write_to_log(&mut self.lm);
        self.lm.flush(lsn).unwrap();
    }

    pub fn set_int(&mut self, buff: &Buffer, offset: i32, _new_value: i32) -> i32 {
        let old_value = buff.contents.get_int(offset);
        let block = buff.block().clone().unwrap();
        SetIntRecord::write_to_log(&mut self.lm, self.tx_num, block, offset, old_value)
    }

    pub fn set_bytes(&mut self, buff: &Buffer, offset: i32, _new_value: &[u8]) -> i32 {
        let old_value = buff.contents.get_bytes(offset);
        let block = buff.block().clone().unwrap();
        SetBytesRecord::write_to_log(&mut self.lm, self.tx_num, block, offset, old_value)
    }

    pub fn set_bool(&mut self, buff: &Buffer, offset: i32, _new_value: bool) -> i32 {
        let old_value = buff.contents.get_bool(offset);
        let block = buff.block().clone().unwrap();
        SetBoolRecord::write_to_log(&mut self.lm, self.tx_num, block, offset, old_value)
    }

    pub fn set_string(&mut self, buff: &Buffer, offset: i32, _new_value: &str) -> i32 {
        let old_value = buff.contents.get_string(offset);
        let block = buff.block().clone().unwrap();
        SetStringRecord::write_to_log(&mut self.lm, self.tx_num, block, offset, &old_value)
    }

    pub fn set_double(&mut self, buff: &Buffer, offset: i32, _new_value: f64) -> i32 {
        let old_value = buff.contents.get_double(offset);
        let block = buff.block().clone().unwrap();
        SetDoubleRecord::write_to_log(&mut self.lm, self.tx_num, block, offset, old_value)
    }

    fn do_rollback(&mut self, tx: &mut Transaction) {
        for bytes in self.lm.iter().unwrap() {
            let rec = create_log_record(bytes).unwrap();
            if rec.tx_num() == self.tx_num {
                if rec.op() == START {
                    return;
                }
                rec.undo(tx);
            }
        }
    }

    fn do_recover(&mut self, tx: &mut Transaction) {
        let mut finished_txs = vec![];
        for bytes in self.lm.iter().unwrap() {
            let rec = create_log_record(bytes).unwrap();
            if rec.op() == CHECKPOINT {
                return;
            } else if rec.op() == COMMIT || rec.op() == ROLLBACK {
                finished_txs.push(rec.tx_num());
            } else if !finished_txs.contains(&rec.tx_num()) {
                rec.undo(tx);
            }
        }
    }
}
