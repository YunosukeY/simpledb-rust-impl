#![allow(dead_code)]

use crate::{
    file::{block_id::BlockId, page::Page},
    log::log_manager::LogManager,
    tx::transaction::Transaction,
    util::Result,
};

use super::log_record::{LogRecord, SET_STRING};

#[derive(PartialEq, Debug)]
pub struct SetStringRecord {
    tx_num: i32,
    offset: i32,
    old_value: String,
    block: BlockId,
}

impl SetStringRecord {
    pub fn new(tx_num: i32, block: BlockId, offset: i32, old_value: &str) -> Self {
        Self {
            tx_num,
            offset,
            old_value: old_value.to_string(),
            block,
        }
    }

    pub fn write_to_log(&self, lm: &mut LogManager) -> Result<i32> {
        let page = Page::from(self);
        lm.append(page.buffer())
    }
}

impl From<Page> for SetStringRecord {
    fn from(page: Page) -> Self {
        let tpos = 4;
        let tx_num = page.get_int(tpos);

        let fpos = tpos + 4;
        let filename = page.get_string(fpos);

        let bpos = fpos + Page::str_len(&filename);
        let block_num = page.get_int(bpos);

        let opos = bpos + 4;
        let offset = page.get_int(opos);

        let vpos = opos + 4;
        let old_value = page.get_string(vpos);

        Self {
            tx_num,
            offset,
            old_value,
            block: BlockId::new(filename, block_num),
        }
    }
}
impl From<&SetStringRecord> for Page {
    fn from(record: &SetStringRecord) -> Self {
        let tpos = 4;
        let fpos = tpos + 4;
        let bpos = fpos + Page::str_len(record.block.filename());
        let opos = bpos + 4;
        let vpos = opos + 4;

        let mut page = Page::new(vpos + Page::str_len(&record.old_value));

        page.set_int(0, SET_STRING);
        page.set_int(tpos, record.tx_num);
        page.set_string(fpos, record.block.filename());
        page.set_int(bpos, record.block.block_num());
        page.set_int(opos, record.offset);
        page.set_string(vpos, &record.old_value);

        page
    }
}

impl LogRecord for SetStringRecord {
    fn op(&self) -> i32 {
        SET_STRING
    }

    fn tx_num(&self) -> i32 {
        self.tx_num
    }

    fn undo(&self, tx: &mut Transaction) {
        tx.pin(&self.block).unwrap();
        tx.set_string(&self.block, self.offset, &self.old_value, false)
            .unwrap();
        tx.unpin(&self.block);
    }
}

impl std::fmt::Display for SetStringRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "<SET_STRING {} {} {} {}>",
            self.tx_num, self.block, self.offset, self.old_value
        )
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test() {
        let record =
            SetStringRecord::new(1, BlockId::new("filename".to_string(), 2), 3, "old_value");

        let record2 = SetStringRecord::from(Page::from(&record));

        assert_eq!(record, record2);
    }

    #[test]
    fn to_string() {
        let record =
            SetStringRecord::new(1, BlockId::new("filename".to_string(), 2), 3, "old_value");

        assert_eq!(
            format!("{}", record),
            "<SET_STRING 1 [file filename, block 2] 3 old_value>"
        );
    }
}
