#![allow(dead_code)]

use crate::{
    file::{block_id::BlockId, page::Page},
    log::log_manager::LogManager,
    tx::transaction::Transaction,
    util::Result,
};

use super::log_record::{LogRecord, SET_BYTES};

#[derive(PartialEq, Debug)]
pub struct SetBytesRecord {
    tx_num: i32,
    offset: i32,
    old_value: Vec<u8>,
    block: BlockId,
}

impl SetBytesRecord {
    pub fn new(tx_num: i32, block: BlockId, offset: i32, old_value: &[u8]) -> Self {
        Self {
            tx_num,
            offset,
            old_value: old_value.to_vec(),
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
        let old_value: Vec<u8> = page.get_bytes(vpos).to_vec();

        Self {
            tx_num,
            offset,
            old_value,
            block: BlockId::new(filename, block_num),
        }
    }

    fn page(&self) -> Page {
        let tpos = 4;
        let fpos = tpos + 4;
        let bpos = fpos + Page::str_len(self.block.filename());
        let opos = bpos + 4;
        let vpos = opos + 4;

        let rec = vec![0; (vpos + Page::bytes_len(&self.old_value)) as usize];
        let mut page = Page::from_bytes(&rec);

        page.set_int(0, SET_BYTES);
        page.set_int(tpos, self.tx_num);
        page.set_string(fpos, self.block.filename());
        page.set_int(bpos, self.block.block_num());
        page.set_int(opos, self.offset);
        page.set_bytes(vpos, &self.old_value);

        page
    }

    pub fn write_to_log(&self, lm: &mut LogManager) -> Result<i32> {
        let page = self.page();
        lm.append(page.buffer())
    }
}

impl LogRecord for SetBytesRecord {
    fn op(&self) -> i32 {
        SET_BYTES
    }

    fn tx_num(&self) -> i32 {
        self.tx_num
    }

    fn undo(&self, tx: &mut Transaction) {
        tx.pin(&self.block).unwrap();
        tx.set_bytes(&self.block, self.offset, &self.old_value, false)
            .unwrap();
        tx.unpin(&self.block);
    }
}

impl std::fmt::Display for SetBytesRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "<SET_BYTES {} {} {} {:?}>",
            self.tx_num, self.block, self.offset, self.old_value
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test() {
        let record = SetBytesRecord::new(1, BlockId::new("filename".to_string(), 2), 3, &[4, 5, 6]);

        let record2 = SetBytesRecord::from_page(record.page());

        assert_eq!(record, record2);
    }

    #[test]
    fn to_string() {
        let record = SetBytesRecord::new(1, BlockId::new("filename".to_string(), 2), 3, &[4, 5, 6]);

        assert_eq!(
            record.to_string(),
            "<SET_BYTES 1 [file filename, block 2] 3 [4, 5, 6]>"
        );
    }
}
