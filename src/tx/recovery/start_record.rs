#![allow(dead_code)]

use crate::{
    file::page::Page,
    log::log_manager::LogManager,
    tx::transaction::Transaction,
    util::{Result, INTEGER_BYTES},
};

use super::log_record::{LogRecord, START};

#[derive(PartialEq, Debug)]
pub struct StartRecord {
    tx_num: i32,
}

impl StartRecord {
    pub fn new(tx_num: i32) -> Self {
        Self { tx_num }
    }

    pub fn write_to_log(&self, lm: &mut LogManager) -> Result<i32> {
        let page = Page::from(self);
        lm.append(page.buffer())
    }
}
impl From<Page> for StartRecord {
    fn from(page: Page) -> Self {
        let tx_num = page.get_int(INTEGER_BYTES);
        StartRecord { tx_num }
    }
}
impl From<&StartRecord> for Page {
    fn from(record: &StartRecord) -> Self {
        let mut page = Page::new(2 * INTEGER_BYTES);
        page.set_int(0, START);
        page.set_int(INTEGER_BYTES, record.tx_num);
        page
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test() {
        let record = StartRecord::new(1);

        let record2 = StartRecord::from(Page::from(&record));

        assert_eq!(record, record2);
    }

    #[test]
    fn to_string() {
        let record = StartRecord::new(1);

        assert_eq!(record.to_string(), "<START 1>");
    }
}
