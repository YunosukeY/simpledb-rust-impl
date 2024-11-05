#![allow(dead_code)]

use crate::{file::page::Page, log::log_manager::LogManager, tx::transaction::Transaction};

use super::log_record::{LogRecord, CHECKPOINT};

pub struct CheckpointRecord {}

impl CheckpointRecord {
    pub fn new() -> CheckpointRecord {
        CheckpointRecord {}
    }

    pub fn page(&self) -> Page {
        let rec = vec![0; 4];
        let mut page = Page::from_bytes(&rec);
        page.set_int(0, CHECKPOINT);
        page
    }

    pub fn write_to_log(&self, lm: &mut LogManager) -> i32 {
        let page = self.page();
        lm.append(page.buffer()).unwrap()
    }
}

impl LogRecord for CheckpointRecord {
    fn op(&self) -> i32 {
        CHECKPOINT
    }

    fn tx_num(&self) -> i32 {
        -1
    }

    fn undo(&self, _tx: &mut Transaction) {}
}

impl std::fmt::Display for CheckpointRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "<CHECKPOINT>")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn page() {
        let record = CheckpointRecord::new();
        let page = record.page();
        assert_eq!(page.buffer(), vec![0, 0, 0, 0]);
    }

    #[test]
    fn to_string() {
        let record = CheckpointRecord::new();
        assert_eq!(record.to_string(), "<CHECKPOINT>");
    }
}
