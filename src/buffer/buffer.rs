#![allow(dead_code)]

use std::sync::Arc;

use crate::{
    file::{block_id::BlockId, file_manager::FileManager, page::Page},
    log::log_manager::LogManager,
    util::Result,
};

pub struct Buffer {
    fm: Arc<FileManager>,
    lm: Arc<LogManager>,
    pub contents: Page,
    block: Option<BlockId>,
    pins: i32,
    tx_num: i32,
    lsn: i32,
}

impl Buffer {
    pub fn new(fm: Arc<FileManager>, lm: Arc<LogManager>) -> Self {
        let contents = Page::new(fm.block_size());
        Buffer {
            fm,
            lm,
            contents,
            block: None,
            pins: 0,
            tx_num: -1,
            lsn: -1,
        }
    }

    pub fn block(&self) -> &Option<BlockId> {
        &self.block
    }

    pub fn set_modified(&mut self, tx_num: i32, lsn: i32) {
        self.tx_num = tx_num;
        if lsn >= 0 {
            self.lsn = lsn;
        }
    }

    pub fn is_pinned(&self) -> bool {
        self.pins > 0
    }

    pub fn modifying_tx(&self) -> i32 {
        self.tx_num
    }

    pub(super) fn assign_to_block(&mut self, block: BlockId) -> Result<()> {
        self.flush()?;
        self.block = Some(block);
        let fm = Arc::as_ptr(&self.fm) as *mut FileManager;
        unsafe {
            (*fm).read(self.block.as_ref().unwrap(), &mut self.contents)?;
        }
        self.pins = 0;
        Ok(())
    }

    pub(super) fn flush(&mut self) -> Result<()> {
        if self.tx_num >= 0 {
            let lm = Arc::as_ptr(&self.lm) as *mut LogManager;
            unsafe {
                (*lm).flush(self.lsn)?;
            }
            let fm = Arc::as_ptr(&self.fm) as *mut FileManager;
            unsafe {
                (*fm).write(self.block.as_ref().unwrap(), &self.contents)?;
            }
            self.tx_num = -1;
        }
        Ok(())
    }

    pub(super) fn pin(&mut self) {
        self.pins += 1;
    }

    pub(super) fn unpin(&mut self) {
        self.pins -= 1;
    }
}
