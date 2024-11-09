#![allow(dead_code)]

use std::collections::HashSet;

use crate::{
    file::page::Page,
    log::log_manager::LogManager,
    tx::transaction::Transaction,
    util::{Result, INTEGER_BYTES},
};

use super::log_record::{LogRecord, NQCKPT};

pub struct NqCkptRecord {
    tx_nums: Vec<i32>,
}

// Nonquiescent Checkpoint Record
impl NqCkptRecord {
    pub fn new(tx_nums: Vec<i32>) -> NqCkptRecord {
        NqCkptRecord { tx_nums }
    }

    pub fn tx_nums(&self) -> HashSet<i32> {
        self.tx_nums.iter().cloned().collect()
    }

    pub fn write_to_log(&self, lm: &mut LogManager) -> Result<i32> {
        let page = Page::from(self);
        lm.append(page.buffer())
    }
}

impl From<Page> for NqCkptRecord {
    fn from(page: Page) -> Self {
        let vpos = INTEGER_BYTES;
        let tx_nums: Vec<i32> = page
            .get_bytes(vpos)
            .chunks(INTEGER_BYTES as usize)
            .map(|b| i32::from_be_bytes(b.try_into().unwrap()))
            .collect();

        Self { tx_nums }
    }
}
impl From<&NqCkptRecord> for Page {
    fn from(record: &NqCkptRecord) -> Self {
        let bytes: Vec<u8> = record
            .tx_nums
            .iter()
            .map(|t| t.to_be_bytes())
            .flatten()
            .collect();

        let vpos = INTEGER_BYTES;

        let mut page = Page::new(vpos + Page::bytes_len(&bytes));
        page.set_int(0, NQCKPT);
        page.set_bytes(vpos, &bytes);

        page
    }
}

impl LogRecord for NqCkptRecord {
    fn op(&self) -> i32 {
        NQCKPT
    }

    fn tx_num(&self) -> i32 {
        -1
    }

    fn undo(&self, _tx: &mut Transaction) {}
}

impl std::fmt::Display for NqCkptRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "<NQCKPT {:?}>", self.tx_nums)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test() {
        let record = NqCkptRecord::new(vec![1, 2, 3]);

        let record2 = NqCkptRecord::from(Page::from(&record));

        assert_eq!(record.tx_nums, record2.tx_nums);
    }

    #[test]
    fn to_string() {
        let record = NqCkptRecord::new(vec![1, 2, 3]);
        assert_eq!(record.to_string(), "<NQCKPT [1, 2, 3]>");
    }
}
