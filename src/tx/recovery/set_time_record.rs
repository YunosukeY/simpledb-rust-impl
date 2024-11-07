#![allow(dead_code)]

use crate::{
    file::{block_id::BlockId, page::Page},
    log::log_manager::LogManager,
    tx::transaction::Transaction,
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

    pub fn from_page(page: Page) -> Self {
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

    pub fn page(&self) -> Page {
        let tpos = 4;
        let fpos = tpos + 4;
        let bpos = fpos + Page::str_len(self.block.filename());
        let opos = bpos + 4;
        let vpos = opos + 4;

        let rec = vec![0; (vpos + Page::time_len(&self.old_value)) as usize];
        let mut page = Page::from_bytes(&rec);

        page.set_int(0, SET_TIME);
        page.set_int(tpos, self.tx_num);
        page.set_string(fpos, self.block.filename());
        page.set_int(bpos, self.block.block_num());
        page.set_int(opos, self.offset);
        page.set_time(vpos, &self.old_value);

        page
    }

    pub fn write_to_log(&self, lm: &mut LogManager) -> i32 {
        let page = self.page();
        lm.append(page.buffer()).unwrap()
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
        tx.pin(&self.block);
        tx.set_time(&self.block, self.offset, &self.old_value, false);
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

        let record2 = SetTimeRecord::from_page(record.page());

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
