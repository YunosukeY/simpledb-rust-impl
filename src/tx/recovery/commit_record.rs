#![allow(dead_code)]

use crate::{file::page::Page, log::log_manager::LogManager, tx::transaction::Transaction};

use super::log_record::{LogRecord, COMMIT};

#[derive(PartialEq, Debug)]
pub struct CommitRecord {
    tx_num: i32,
}

impl CommitRecord {
    pub fn new(tx_num: i32) -> Self {
        Self { tx_num }
    }

    pub fn from_page(page: Page) -> Self {
        let tx_num = page.get_int(4);
        CommitRecord { tx_num }
    }

    pub fn page(&self) -> Page {
        let rec = vec![0; 8];
        let mut page = Page::from_bytes(&rec);
        page.set_int(0, COMMIT);
        page.set_int(4, self.tx_num);
        page
    }

    pub fn write_to_log(&self, lm: &mut LogManager) -> i32 {
        let page = self.page();
        lm.append(page.buffer()).unwrap()
    }
}

impl LogRecord for CommitRecord {
    fn op(&self) -> i32 {
        COMMIT
    }

    fn tx_num(&self) -> i32 {
        self.tx_num
    }

    fn undo(&self, _tx: &mut Transaction) {}
}

impl std::fmt::Display for CommitRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "<COMMIT {}>", self.tx_num)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test() {
        let record = CommitRecord::new(1);

        let record2 = CommitRecord::from_page(record.page());

        assert_eq!(record, record2);
    }

    #[test]
    fn to_string() {
        let record = CommitRecord::new(1);

        assert_eq!(format!("{}", record), "<COMMIT 1>");
    }
}
