#![allow(dead_code)]

use std::vec;

use crate::{
    buffer::{buffer::Buffer, buffer_manager::BufferManager},
    log::log_manager::LogManager,
    tx::transaction::Transaction,
};

use super::{
    checkpoint_record::CheckpointRecord,
    commit_record::CommitRecord,
    log_record::{create_log_record, CHECKPOINT, COMMIT, ROLLBACK, START},
    rollback_record::RollbackRecord,
    set_int_record::SetIntRecord,
    set_string_record::SetStringRecord,
    start_record::StartRecord,
};

pub struct RecoveryManager {
    lm: LogManager,
    bm: BufferManager,
    tx: Transaction,
    tx_num: i32,
}

impl RecoveryManager {
    pub fn new(tx: Transaction, tx_num: i32, mut lm: LogManager, bm: BufferManager) -> Self {
        StartRecord::write_to_log(&mut lm, tx_num);
        Self { tx, tx_num, lm, bm }
    }

    pub fn commit(&mut self) {
        self.bm.flush_all(self.tx_num).unwrap();
        let lsn = CommitRecord::write_to_log(&mut self.lm, self.tx_num);
        self.lm.flush(lsn).unwrap();
    }

    pub fn rollback(&mut self) {
        self.do_rollback();
        self.bm.flush_all(self.tx_num).unwrap();
        let lsn = RollbackRecord::write_to_log(&mut self.lm, self.tx_num);
        self.lm.flush(lsn).unwrap();
    }

    pub fn recover(&mut self) {
        self.do_recover();
        self.bm.flush_all(self.tx_num).unwrap();
        let lsn = CheckpointRecord::write_to_log(&mut self.lm);
        self.lm.flush(lsn).unwrap();
    }

    pub fn set_int(&mut self, mut buff: Buffer, offset: i32, _new_value: i32) -> i32 {
        let old_value = buff.contents().get_int(offset);
        let block = buff.block().clone().unwrap();
        SetIntRecord::write_to_log(&mut self.lm, self.tx_num, block, offset, old_value)
    }

    pub fn set_string(&mut self, mut buff: Buffer, offset: i32, _new_value: &str) -> i32 {
        let old_value = buff.contents().get_string(offset);
        let block = buff.block().clone().unwrap();
        SetStringRecord::write_to_log(&mut self.lm, self.tx_num, block, offset, &old_value)
    }

    fn do_rollback(&mut self) {
        for bytes in self.lm.iter().unwrap() {
            let rec = create_log_record(bytes).unwrap();
            if rec.tx_num() == self.tx_num {
                if rec.op() == START {
                    return;
                }
                rec.undo(&mut self.tx);
            }
        }
    }

    fn do_recover(&mut self) {
        let mut finished_txs = vec![];
        for bytes in self.lm.iter().unwrap() {
            let rec = create_log_record(bytes).unwrap();
            if rec.op() == CHECKPOINT {
                return;
            } else if rec.op() == COMMIT || rec.op() == ROLLBACK {
                finished_txs.push(rec.tx_num());
            } else if !finished_txs.contains(&rec.tx_num()) {
                rec.undo(&mut self.tx);
            }
        }
    }
}
