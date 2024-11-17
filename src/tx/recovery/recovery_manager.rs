#![allow(dead_code)]

use std::{collections::HashSet, sync::Arc, vec};

use crate::{
    buffer::{buffer::Buffer, buffer_manager::BufferManager},
    file::page::Page,
    log::log_manager::LogManager,
    tx::{
        recovery::{
            set_bool_record::SetBoolRecord, set_datetime_record::SetDatetimeRecord,
            set_double_record::SetDoubleRecord, set_json_record::SetJsonRecord,
        },
        transaction::Transaction,
    },
    util::Result,
};

use super::{
    checkpoint_record::CheckpointRecord,
    commit_record::CommitRecord,
    log_record::{create_log_record, CHECKPOINT, COMMIT, NQCKPT, ROLLBACK, START},
    nq_ckpt_record::NqCkptRecord,
    rollback_record::RollbackRecord,
    set_bytes_record::SetBytesRecord,
    set_date_record::SetDateRecord,
    set_int_record::SetIntRecord,
    set_string_record::SetStringRecord,
    set_time_record::SetTimeRecord,
    start_record::StartRecord,
};

pub struct RecoveryManager {
    lm: Arc<LogManager>,
    bm: Arc<BufferManager>,
    tx_num: i32,
}

impl RecoveryManager {
    pub fn new(tx_num: i32, lm: Arc<LogManager>, bm: Arc<BufferManager>) -> Self {
        {
            let lm = Arc::as_ptr(&lm) as *mut LogManager;
            unsafe {
                StartRecord::new(tx_num).write_to_log(&mut *lm).unwrap();
            }
        }
        Self { tx_num, lm, bm }
    }

    pub fn commit(&mut self) -> Result<()> {
        let bm = Arc::as_ptr(&self.bm) as *mut BufferManager;
        unsafe {
            (*bm).flush_all(self.tx_num)?;
        }
        let lm = Arc::as_ptr(&self.lm) as *mut LogManager;
        unsafe {
            let lsn = CommitRecord::new(self.tx_num).write_to_log(&mut *lm)?;
            (*lm).flush(lsn)?;
        }
        Ok(())
    }

    pub fn rollback(&mut self, tx: &mut Transaction) {
        self.do_rollback(tx);
        let bm = Arc::as_ptr(&self.bm) as *mut BufferManager;
        unsafe {
            (*bm).flush_all(self.tx_num).unwrap();
        }
        let lm = Arc::as_ptr(&self.lm) as *mut LogManager;
        unsafe {
            let lsn = RollbackRecord::new(self.tx_num)
                .write_to_log(&mut *lm)
                .unwrap();
            (*lm).flush(lsn).unwrap();
        }
    }

    pub fn recover(&mut self, tx: &mut Transaction) {
        self.do_recover(tx);
        let bm = Arc::as_ptr(&self.bm) as *mut BufferManager;
        unsafe {
            (*bm).flush_all(self.tx_num).unwrap();
        }
        let lm = Arc::as_ptr(&self.lm) as *mut LogManager;
        unsafe {
            let lsn = CheckpointRecord::new().write_to_log(&mut *lm).unwrap();
            (*lm).flush(lsn).unwrap();
        }
    }

    pub fn set_int(&mut self, buff: &Buffer, offset: i32, _new_value: i32) -> Result<i32> {
        let old_value = buff.contents.get_int(offset);
        let block = buff.block().clone().unwrap();
        let lm = Arc::as_ptr(&self.lm) as *mut LogManager;
        unsafe { SetIntRecord::new(self.tx_num, block, offset, old_value).write_to_log(&mut *lm) }
    }

    pub fn set_bytes(&mut self, buff: &Buffer, offset: i32, _new_value: &[u8]) -> Result<i32> {
        let old_value = buff.contents.get_bytes(offset);
        let block = buff.block().clone().unwrap();
        let lm = Arc::as_ptr(&self.lm) as *mut LogManager;
        unsafe { SetBytesRecord::new(self.tx_num, block, offset, old_value).write_to_log(&mut *lm) }
    }

    pub fn set_bool(&mut self, buff: &Buffer, offset: i32, _new_value: bool) -> Result<i32> {
        let old_value = buff.contents.get_bool(offset);
        let block = buff.block().clone().unwrap();
        let lm = Arc::as_ptr(&self.lm) as *mut LogManager;
        unsafe { SetBoolRecord::new(self.tx_num, block, offset, old_value).write_to_log(&mut *lm) }
    }

    pub fn set_string(&mut self, buff: &Buffer, offset: i32, _new_value: &str) -> Result<i32> {
        let old_value = buff.contents.get_string(offset);
        let block = buff.block().clone().unwrap();
        let lm = Arc::as_ptr(&self.lm) as *mut LogManager;
        unsafe {
            SetStringRecord::new(self.tx_num, block, offset, &old_value).write_to_log(&mut *lm)
        }
    }

    pub fn set_double(&mut self, buff: &Buffer, offset: i32, _new_value: f64) -> Result<i32> {
        let old_value = buff.contents.get_double(offset);
        let block = buff.block().clone().unwrap();
        let lm = Arc::as_ptr(&self.lm) as *mut LogManager;
        unsafe {
            SetDoubleRecord::new(self.tx_num, block, offset, old_value).write_to_log(&mut *lm)
        }
    }

    pub fn set_date(
        &mut self,
        buff: &Buffer,
        offset: i32,
        _new_value: &Option<chrono::NaiveDate>,
    ) -> Result<i32> {
        let old_value = buff.contents.get_date(offset);
        let block = buff.block().clone().unwrap();
        let lm = Arc::as_ptr(&self.lm) as *mut LogManager;
        unsafe { SetDateRecord::new(self.tx_num, block, offset, old_value).write_to_log(&mut *lm) }
    }

    pub fn set_time(
        &mut self,
        buff: &Buffer,
        offset: i32,
        _new_value: &Option<chrono::NaiveTime>,
    ) -> Result<i32> {
        let old_value = buff.contents.get_time(offset);
        let block = buff.block().clone().unwrap();
        let lm = Arc::as_ptr(&self.lm) as *mut LogManager;
        unsafe { SetTimeRecord::new(self.tx_num, block, offset, old_value).write_to_log(&mut *lm) }
    }

    pub fn set_datetime(
        &mut self,
        buff: &Buffer,
        offset: i32,
        _new_value: &Option<chrono::DateTime<chrono::FixedOffset>>,
    ) -> Result<i32> {
        let old_value = buff.contents.get_datetime(offset);
        let block = buff.block().clone().unwrap();
        let lm = Arc::as_ptr(&self.lm) as *mut LogManager;
        unsafe {
            SetDatetimeRecord::new(self.tx_num, block, offset, old_value).write_to_log(&mut *lm)
        }
    }

    pub fn set_json(
        &mut self,
        buff: &Buffer,
        offset: i32,
        _new_value: &Option<serde_json::Value>,
    ) -> Result<i32> {
        let old_value = buff.contents.get_json(offset);
        let block = buff.block().clone().unwrap();
        let lm = Arc::as_ptr(&self.lm) as *mut LogManager;
        unsafe { SetJsonRecord::new(self.tx_num, block, offset, &old_value).write_to_log(&mut *lm) }
    }

    fn do_rollback(&mut self, tx: &mut Transaction) {
        let lm = Arc::as_ptr(&self.lm) as *mut LogManager;
        for bytes in unsafe { (*lm).iter().unwrap() } {
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
        let lm = Arc::as_ptr(&self.lm) as *mut LogManager;
        let mut finished_txs = vec![];
        let mut unfinished_txs: Option<HashSet<i32>> = None;
        for bytes in unsafe { (*lm).iter().unwrap() } {
            let rec = create_log_record(bytes.clone()).unwrap();
            if rec.op() == CHECKPOINT {
                return;
            } else if rec.op() == NQCKPT && unfinished_txs.is_none() {
                let rec = NqCkptRecord::from(Page::from(bytes));
                unfinished_txs = Some(rec.tx_nums());
            } else if rec.op() == START {
                if let Some(unfinished_txs) = &mut unfinished_txs {
                    unfinished_txs.remove(&rec.tx_num());
                    if unfinished_txs.is_empty() {
                        return;
                    }
                }
            } else if rec.op() == COMMIT || rec.op() == ROLLBACK {
                finished_txs.push(rec.tx_num());
            } else if !finished_txs.contains(&rec.tx_num()) {
                rec.undo(tx);
            }
        }
    }
}
