#![allow(dead_code)]

use crate::tx::transaction::Transaction;

pub static CHECKPOINT: i32 = 0;
pub static START: i32 = 1;
pub static COMMIT: i32 = 2;
pub static ROLLBACK: i32 = 3;
pub static SET_INT: i32 = 4;
pub static SET_STRING: i32 = 5;

pub trait LogRecord {
    fn op(&self) -> i32;

    fn tx_num(&self) -> i32;

    fn undo(&self, tx: Transaction);

    // TODO
    fn create_log_record(bytes: Vec<u8>) {}
}
