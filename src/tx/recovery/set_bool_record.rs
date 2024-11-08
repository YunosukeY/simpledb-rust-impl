#![allow(dead_code)]

use crate::{
    file::{block_id::BlockId, page::Page},
    log::log_manager::LogManager,
    tx::transaction::Transaction,
    util::Result,
};

use super::log_record::{LogRecord, SET_BOOL};

#[derive(PartialEq, Debug)]
pub struct SetBoolRecord {
    tx_num: i32,
    offset: i32,
    old_value: bool,
    block: BlockId,
}

impl SetBoolRecord {
    pub fn new(tx_num: i32, block: BlockId, offset: i32, old_value: bool) -> Self {
        Self {
            tx_num,
            offset,
            old_value,
            block,
        }
    }

    pub fn write_to_log(&self, lm: &mut LogManager) -> Result<i32> {
        let page = Page::from(self);
        lm.append(page.buffer())
    }
}

impl From<Page> for SetBoolRecord {
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
        let old_value = page.get_bool(vpos);

        Self {
            tx_num,
            offset,
            old_value,
            block: BlockId::new(filename, block_num),
        }
    }
}
impl From<&SetBoolRecord> for Page {
    fn from(record: &SetBoolRecord) -> Self {
        let tpos = 4;
        let fpos = tpos + 4;
        let bpos = fpos + Page::str_len(record.block.filename());
        let opos = bpos + 4;
        let vpos = opos + 4;

        let mut page = Page::new(vpos + Page::bool_len(record.old_value));

        page.set_int(0, SET_BOOL);
        page.set_int(tpos, record.tx_num);
        page.set_string(fpos, record.block.filename());
        page.set_int(bpos, record.block.block_num());
        page.set_int(opos, record.offset);
        page.set_bool(vpos, record.old_value);

        page
    }
}

impl LogRecord for SetBoolRecord {
    fn op(&self) -> i32 {
        SET_BOOL
    }

    fn tx_num(&self) -> i32 {
        self.tx_num
    }

    fn undo(&self, tx: &mut Transaction) {
        tx.pin(&self.block).unwrap();
        tx.set_bool(&self.block, self.offset, self.old_value, false)
            .unwrap();
        tx.unpin(&self.block);
    }
}

impl std::fmt::Display for SetBoolRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "<SET_BOOL {} {} {} {}>",
            self.tx_num, self.block, self.offset, self.old_value
        )
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test() {
        let record = SetBoolRecord::new(1, BlockId::new("filename".to_string(), 2), 3, true);

        let record2 = SetBoolRecord::from(Page::from(&record));

        assert_eq!(record, record2);
    }

    #[test]
    fn to_string() {
        let record = SetBoolRecord::new(1, BlockId::new("filename".to_string(), 2), 3, true);

        assert_eq!(
            record.to_string(),
            "<SET_BOOL 1 [file filename, block 2] 3 true>"
        );
    }
}
