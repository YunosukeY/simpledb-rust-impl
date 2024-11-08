#![allow(dead_code)]

use crate::{
    file::{block_id::BlockId, page::Page},
    log::log_manager::LogManager,
    tx::transaction::Transaction,
    util::{Result, INTEGER_BYTES},
};

use super::log_record::{LogRecord, SET_DOUBLE};

#[derive(PartialEq, Debug)]
pub struct SetDoubleRecord {
    tx_num: i32,
    offset: i32,
    old_value: f64,
    block: BlockId,
}

impl SetDoubleRecord {
    pub fn new(tx_num: i32, block: BlockId, offset: i32, old_value: f64) -> Self {
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

impl From<Page> for SetDoubleRecord {
    fn from(page: Page) -> Self {
        let tpos = INTEGER_BYTES;
        let tx_num = page.get_int(tpos);

        let fpos = tpos + INTEGER_BYTES;
        let filename = page.get_string(fpos);

        let bpos = fpos + Page::str_len(&filename);
        let block_num = page.get_int(bpos);

        let opos = bpos + INTEGER_BYTES;
        let offset = page.get_int(opos);

        let vpos = opos + INTEGER_BYTES;
        let old_value = page.get_double(vpos);

        Self {
            tx_num,
            offset,
            old_value,
            block: BlockId::new(filename, block_num),
        }
    }
}
impl From<&SetDoubleRecord> for Page {
    fn from(record: &SetDoubleRecord) -> Self {
        let tpos = INTEGER_BYTES;
        let fpos = tpos + INTEGER_BYTES;
        let bpos = fpos + Page::str_len(record.block.filename());
        let opos = bpos + INTEGER_BYTES;
        let vpos = opos + INTEGER_BYTES;

        let mut page = Page::new(vpos + Page::double_len(record.old_value));

        page.set_int(0, SET_DOUBLE);
        page.set_int(tpos, record.tx_num);
        page.set_string(fpos, record.block.filename());
        page.set_int(bpos, record.block.block_num());
        page.set_int(opos, record.offset);
        page.set_double(vpos, record.old_value);

        page
    }
}

impl LogRecord for SetDoubleRecord {
    fn op(&self) -> i32 {
        SET_DOUBLE
    }

    fn tx_num(&self) -> i32 {
        self.tx_num
    }

    fn undo(&self, tx: &mut Transaction) {
        tx.pin(&self.block).unwrap();
        tx.set_double(&self.block, self.offset, self.old_value, false)
            .unwrap();
        tx.unpin(&self.block);
    }
}

impl std::fmt::Display for SetDoubleRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "<SET_DOUBLE {} {} {} {}>",
            self.tx_num, self.block, self.offset, self.old_value
        )
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test() {
        let record = SetDoubleRecord::new(3, BlockId::new("filename".to_string(), 2), 4, 5.0);

        let record2 = SetDoubleRecord::from(Page::from(&record));

        assert_eq!(record, record2);
    }

    #[test]
    fn to_string() {
        let record = SetDoubleRecord::new(3, BlockId::new("filename".to_string(), 2), 4, 5.6);

        assert_eq!(
            format!("{}", record),
            "<SET_DOUBLE 3 [file filename, block 2] 4 5.6>"
        );
    }
}
