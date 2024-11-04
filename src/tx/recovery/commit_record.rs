#![allow(dead_code)]

use crate::{file::page::Page, log::log_manager::LogManager, tx::transaction::Transaction};

use super::log_record::{LogRecord, COMMIT};

pub struct CommitRecord {
    tx_num: i32,
}

impl CommitRecord {
    pub fn new(page: Page) -> Self {
        let tx_num = page.get_int(4);
        CommitRecord { tx_num }
    }

    pub fn write_to_log(lm: &mut LogManager, tx_num: i32) -> i32 {
        let rec = vec![0; 8];
        let mut p = Page::from_bytes(&rec);
        p.set_int(0, COMMIT);
        p.set_int(4, tx_num);
        lm.append(rec).unwrap()
    }
}

impl LogRecord for CommitRecord {
    fn op(&self) -> i32 {
        COMMIT
    }

    fn tx_num(&self) -> i32 {
        self.tx_num
    }

    fn undo(&self, _tx: Transaction) {}
}

impl std::fmt::Display for CommitRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "<COMMIT {}>", self.tx_num)
    }
}
