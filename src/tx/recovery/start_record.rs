#![allow(dead_code)]

use crate::{file::page::Page, log::log_manager::LogManager, tx::transaction::Transaction};

use super::log_record::{LogRecord, START};

pub struct StartRecord {
    tx_num: i32,
}

impl StartRecord {
    pub fn new(page: Page) -> Self {
        let tx_num = page.get_int(4);
        StartRecord { tx_num }
    }

    pub fn write_to_log(lm: &mut LogManager, tx_num: i32) -> i32 {
        let rec = vec![0; 8];
        let mut p = Page::from_bytes(&rec);
        p.set_int(0, START);
        p.set_int(4, tx_num);
        lm.append(rec).unwrap()
    }
}

impl LogRecord for StartRecord {
    fn op(&self) -> i32 {
        START
    }

    fn tx_num(&self) -> i32 {
        self.tx_num
    }

    fn undo(&self, _tx: &mut Transaction) {}
}

impl std::fmt::Display for StartRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "<START {}>", self.tx_num)
    }
}
