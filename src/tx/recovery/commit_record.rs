#![allow(dead_code)]

use crate::{
    file::page::Page,
    log::log_manager::LogManager,
    tx::transaction::Transaction,
    util::{Result, INTEGER_BYTES},
};

use super::log_record::{LogRecord, COMMIT};

#[derive(PartialEq, Debug)]
pub struct CommitRecord {
    tx_num: i32,
}

impl CommitRecord {
    pub fn new(tx_num: i32) -> Self {
        Self { tx_num }
    }

    pub fn write_to_log(&self, lm: &mut LogManager) -> Result<i32> {
        let page = Page::from(self);
        lm.append(page.buffer())
    }
}

impl From<Page> for CommitRecord {
    fn from(page: Page) -> Self {
        let tx_num = page.get_int(INTEGER_BYTES);
        CommitRecord { tx_num }
    }
}
impl From<&CommitRecord> for Page {
    fn from(record: &CommitRecord) -> Self {
        let mut page = Page::new(2 * INTEGER_BYTES);
        page.set_int(0, COMMIT);
        page.set_int(INTEGER_BYTES, record.tx_num);
        page
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

        let record2 = CommitRecord::from(Page::from(&record));

        assert_eq!(record, record2);
    }

    #[test]
    fn to_string() {
        let record = CommitRecord::new(1);

        assert_eq!(format!("{}", record), "<COMMIT 1>");
    }
}
