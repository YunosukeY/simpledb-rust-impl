#![allow(dead_code)]

use crate::{
    file::{block_id::BlockId, page::Page},
    log::log_manager::LogManager,
    tx::transaction::Transaction,
    util::{Result, INTEGER_BYTES},
};

use super::log_record::{LogRecord, SET_DATE};

#[derive(PartialEq, Debug)]
pub struct SetDateRecord {
    tx_num: i32,
    offset: i32,
    old_value: chrono::NaiveDate,
    block: BlockId,
}

impl SetDateRecord {
    pub fn new(tx_num: i32, block: BlockId, offset: i32, old_value: chrono::NaiveDate) -> Self {
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

impl TryFrom<Page> for SetDateRecord {
    type Error = Box<dyn std::error::Error>;

    fn try_from(page: Page) -> Result<Self> {
        let tpos = INTEGER_BYTES;
        let tx_num = page.get_int(tpos);

        let fpos = tpos + INTEGER_BYTES;
        let filename = page.get_string(fpos);

        let bpos = fpos + Page::str_len(&filename);
        let block_num = page.get_int(bpos);

        let opos = bpos + INTEGER_BYTES;
        let offset = page.get_int(opos);

        let vpos = opos + INTEGER_BYTES;
        let old_value = page.get_date(vpos)?;

        Ok(Self {
            tx_num,
            offset,
            old_value,
            block: BlockId::new(filename, block_num),
        })
    }
}
impl From<&SetDateRecord> for Page {
    fn from(record: &SetDateRecord) -> Self {
        let tpos = INTEGER_BYTES;
        let fpos = tpos + INTEGER_BYTES;
        let bpos = fpos + Page::str_len(record.block.filename());
        let opos = bpos + INTEGER_BYTES;
        let vpos = opos + INTEGER_BYTES;

        let mut page = Page::new(vpos + Page::date_len(&record.old_value));

        page.set_int(0, SET_DATE);
        page.set_int(tpos, record.tx_num);
        page.set_string(fpos, record.block.filename());
        page.set_int(bpos, record.block.block_num());
        page.set_int(opos, record.offset);
        page.set_date(vpos, &record.old_value);

        page
    }
}

impl LogRecord for SetDateRecord {
    fn op(&self) -> i32 {
        SET_DATE
    }

    fn tx_num(&self) -> i32 {
        self.tx_num
    }

    fn undo(&self, tx: &mut Transaction) {
        tx.pin(&self.block).unwrap();
        tx.set_date(&self.block, self.offset, &self.old_value, false)
            .unwrap();
        tx.unpin(&self.block);
    }
}

impl std::fmt::Display for SetDateRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "<SET_DATE {} {} {} {:?}>",
            self.tx_num, self.block, self.offset, self.old_value
        )
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test() {
        let record = SetDateRecord::new(
            1,
            BlockId::new("filename".to_string(), 2),
            3,
            chrono::NaiveDate::from_ymd_opt(2021, 1, 1).unwrap(),
        );

        let record2 = SetDateRecord::try_from(Page::from(&record)).unwrap();

        assert_eq!(record, record2);
    }

    #[test]
    fn to_string() {
        let record = SetDateRecord::new(
            1,
            BlockId::new("filename".to_string(), 2),
            3,
            chrono::NaiveDate::from_ymd_opt(2021, 1, 1).unwrap(),
        );

        assert_eq!(
            record.to_string(),
            "<SET_DATE 1 [file filename, block 2] 3 Some(2021-01-01)>"
        );
    }
}
