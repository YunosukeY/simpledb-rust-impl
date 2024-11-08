#![allow(dead_code)]

use crate::{
    file::{block_id::BlockId, page::Page},
    log::log_manager::LogManager,
    tx::transaction::Transaction,
    util::Result,
};

use super::log_record::{LogRecord, SET_TIME};

#[derive(PartialEq, Debug)]
pub struct SetTimeRecord {
    tx_num: i32,
    offset: i32,
    old_value: Option<chrono::NaiveTime>,
    block: BlockId,
}

impl SetTimeRecord {
    pub fn new(
        tx_num: i32,
        block: BlockId,
        offset: i32,
        old_value: Option<chrono::NaiveTime>,
    ) -> Self {
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

impl From<Page> for SetTimeRecord {
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
        let old_value = page.get_time(vpos);

        Self {
            tx_num,
            offset,
            old_value,
            block: BlockId::new(filename, block_num),
        }
    }
}
impl From<&SetTimeRecord> for Page {
    fn from(record: &SetTimeRecord) -> Self {
        let tpos = 4;
        let fpos = tpos + 4;
        let bpos = fpos + Page::str_len(record.block.filename());
        let opos = bpos + 4;
        let vpos = opos + 4;

        let mut page = Page::new(vpos + Page::time_len(&record.old_value));

        page.set_int(0, SET_TIME);
        page.set_int(tpos, record.tx_num);
        page.set_string(fpos, record.block.filename());
        page.set_int(bpos, record.block.block_num());
        page.set_int(opos, record.offset);
        page.set_time(vpos, &record.old_value);

        page
    }
}

impl LogRecord for SetTimeRecord {
    fn op(&self) -> i32 {
        SET_TIME
    }

    fn tx_num(&self) -> i32 {
        self.tx_num
    }

    fn undo(&self, tx: &mut Transaction) {
        tx.pin(&self.block).unwrap();
        tx.set_time(&self.block, self.offset, &self.old_value, false)
            .unwrap();
        tx.unpin(&self.block);
    }
}

impl std::fmt::Display for SetTimeRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "<SET_TIME {} {} {} {:?}>",
            self.tx_num, self.block, self.offset, self.old_value
        )
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test() {
        let record = SetTimeRecord::new(
            1,
            BlockId::new("filename".to_string(), 2),
            3,
            chrono::NaiveTime::from_hms_opt(4, 5, 6),
        );

        let record2 = SetTimeRecord::from(Page::from(&record));

        assert_eq!(record, record2);
    }

    #[test]
    fn to_string() {
        let record = SetTimeRecord::new(
            1,
            BlockId::new("filename".to_string(), 2),
            3,
            chrono::NaiveTime::from_hms_opt(4, 5, 6),
        );

        assert_eq!(
            record.to_string(),
            "<SET_TIME 1 [file filename, block 2] 3 Some(04:05:06)>"
        );
    }
}
