#![allow(dead_code)]

use crate::{file::page::Page, log::log_manager::LogManager, tx::transaction::Transaction};

use super::log_record::{LogRecord, ROLLBACK};

#[derive(PartialEq, Debug)]
pub struct RollbackRecord {
    tx_num: i32,
}

impl RollbackRecord {
    pub fn new(tx_num: i32) -> Self {
        Self { tx_num }
    }

    pub fn from_page(page: Page) -> Self {
        let tx_num = page.get_int(4);
        RollbackRecord { tx_num }
    }

    pub fn page(&self) -> Page {
        let rec = vec![0; 8];
        let mut page = Page::from_bytes(&rec);
        page.set_int(0, ROLLBACK);
        page.set_int(4, self.tx_num);
        page
    }

    pub fn write_to_log(&self, lm: &mut LogManager) -> i32 {
        let page = self.page();
        lm.append(page.buffer()).unwrap()
    }
}

impl LogRecord for RollbackRecord {
    fn op(&self) -> i32 {
        ROLLBACK
    }

    fn tx_num(&self) -> i32 {
        self.tx_num
    }

    fn undo(&self, _tx: &mut Transaction) {}
}

impl std::fmt::Display for RollbackRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "<ROLLBACK {}>", self.tx_num)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test() {
        let record = RollbackRecord::new(1);

        let record2 = RollbackRecord::from_page(record.page());

        assert_eq!(record, record2);
    }

    #[test]
    fn to_string() {
        let record = RollbackRecord::new(1);

        assert_eq!(format!("{}", record), "<ROLLBACK 1>");
    }
}
