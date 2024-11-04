#![allow(dead_code)]

use crate::{
    file::{block_id::BlockId, page::Page},
    log::log_manager::LogManager,
    tx::transaction::Transaction,
};

use super::log_record::{LogRecord, SET_INT};

pub struct SetIntRecord {
    tx_num: i32,
    offset: i32,
    old_value: i32,
    block: BlockId,
}

impl SetIntRecord {
    pub fn new(page: Page) -> Self {
        let tpos = 4;
        let tx_num = page.get_int(tpos);

        let fpos = tpos + 4;
        let filename = page.get_string(fpos);

        let bpos = fpos + Page::max_length(&filename);
        let block_num = page.get_int(bpos);

        let opos = bpos + 4;
        let offset = page.get_int(opos);

        let vpos = opos + 4;
        let old_value = page.get_int(vpos);

        Self {
            tx_num,
            offset,
            old_value,
            block: BlockId::new(filename, block_num),
        }
    }

    pub fn write_to_log(
        lm: &mut LogManager,
        tx_num: i32,
        block: BlockId,
        offset: i32,
        old_value: i32,
    ) -> i32 {
        let tpos = 4;
        let fpos = tpos + 4;
        let bpos = fpos + Page::max_length(block.filename());
        let opos = bpos + 4;
        let vpos = opos + 4;

        let rec = vec![0; vpos as usize + 4];
        let mut page = Page::from_bytes(&rec);

        page.set_int(0, SET_INT);
        page.set_int(tpos, tx_num);
        page.set_string(fpos, block.filename());
        page.set_int(bpos, block.block_num());
        page.set_int(opos, offset);
        page.set_int(vpos, old_value);

        lm.append(rec).unwrap()
    }
}

impl LogRecord for SetIntRecord {
    fn op(&self) -> i32 {
        SET_INT
    }

    fn tx_num(&self) -> i32 {
        self.tx_num
    }

    fn undo(&self, tx: &mut Transaction) {
        tx.pin(&self.block);
        tx.set_int(&self.block, self.offset, self.old_value, false);
        tx.unpin(&self.block);
    }
}

impl std::fmt::Display for SetIntRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "<SETINT {} {} {} {}>",
            self.tx_num, self.block, self.offset, self.old_value
        )
    }
}