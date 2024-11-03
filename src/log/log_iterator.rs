#![allow(dead_code)]

use crate::{
    file::{block_id::BlockId, file_manager::FileManager, page::Page},
    util::Result,
};

pub(super) struct LogIterator<'a> {
    fm: &'a mut FileManager,
    block: BlockId,
    page: Page,
    current_pos: i32,
    boundary: i32,
}

impl<'a> LogIterator<'a> {
    pub fn new(fm: &'a mut FileManager, block: BlockId) -> Self {
        let page = Page::new(fm.block_size());
        let mut iter = Self {
            fm,
            block,
            page,
            current_pos: 0,
            boundary: 0,
        };
        iter.move_to_block().unwrap();
        iter
    }

    fn has_next(&self) -> bool {
        self.current_pos < self.fm.block_size() || self.block.block_num() > 0
    }

    fn move_to_block(&mut self) -> Result<()> {
        self.fm.read(&self.block, &mut self.page)?;
        self.boundary = self.page.get_int(0);
        self.current_pos = self.boundary;
        Ok(())
    }
}

impl<'a> Iterator for LogIterator<'a> {
    type Item = Vec<u8>;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.has_next() {
            return None;
        }

        if self.current_pos == self.fm.block_size() {
            self.block = BlockId::new(
                self.block.filename().to_string(),
                self.block.block_num() - 1,
            );
            self.move_to_block().unwrap();
        }
        let record = self.page.get_bytes(self.current_pos);
        self.current_pos += record.len() as i32 + 4;
        Some(record.to_vec())
    }
}
