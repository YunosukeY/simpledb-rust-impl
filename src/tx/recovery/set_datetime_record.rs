#![allow(dead_code)]

use crate::{
    file::{block_id::BlockId, page::Page},
    log::log_manager::LogManager,
    tx::transaction::Transaction,
    util::Result,
};

use super::log_record::{LogRecord, SET_DATETIME};

#[derive(PartialEq, Debug)]
pub struct SetDatetimeRecord {
    tx_num: i32,
    offset: i32,
    old_value: Option<chrono::DateTime<chrono::FixedOffset>>,
    block: BlockId,
}

impl SetDatetimeRecord {
    pub fn new(
        tx_num: i32,
        block: BlockId,
        offset: i32,
        old_value: Option<chrono::DateTime<chrono::FixedOffset>>,
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
        let old_value = page.get_datetime(vpos);

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

        let rec = vec![0; (vpos + Page::datetime_len(&self.old_value)) as usize];
        let mut page = Page::from_bytes(&rec);

        page.set_int(0, SET_DATETIME);
        page.set_int(tpos, self.tx_num);
        page.set_string(fpos, self.block.filename());
        page.set_int(bpos, self.block.block_num());
        page.set_int(opos, self.offset);
        page.set_datetime(vpos, &self.old_value);

        page
    }

    pub fn write_to_log(&self, lm: &mut LogManager) -> Result<i32> {
        let page = self.page();
        lm.append(page.buffer())
    }
}

impl LogRecord for SetDatetimeRecord {
    fn op(&self) -> i32 {
        SET_DATETIME
    }

    fn tx_num(&self) -> i32 {
        self.tx_num
    }

    fn undo(&self, tx: &mut Transaction) {
        tx.pin(&self.block).unwrap();
        tx.set_datetime(&self.block, self.offset, &self.old_value, false)
            .unwrap();
        tx.unpin(&self.block);
    }
}

impl std::fmt::Display for SetDatetimeRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "<SET_DATETIME {} {} {} {:?}>",
            self.tx_num, self.block, self.offset, self.old_value
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test() {
        let record = SetDatetimeRecord::new(
            1,
            BlockId::new("filename".to_string(), 2),
            3,
            Some(chrono::Utc::now().fixed_offset()),
        );

        let record2 = SetDatetimeRecord::from_page(record.page());

        assert_eq!(record2, record);
    }

    #[test]
    fn to_string() {
        let record = SetDatetimeRecord::new(
            1,
            BlockId::new("filename".to_string(), 2),
            3,
            Some(chrono::Utc::now().fixed_offset()),
        );

        assert_eq!(
            record.to_string(),
            format!(
                "<SET_DATETIME 1 [file filename, block 2] 3 {:?}>",
                record.old_value
            )
        );
    }
}
