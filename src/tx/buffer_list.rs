#![allow(dead_code)]

use std::{collections::HashMap, sync::Arc};

use crate::{
    buffer::{buffer::Buffer, buffer_manager::BufferManager},
    file::block_id::BlockId,
    util::Result,
};

pub(super) struct BufferList {
    buffers: HashMap<BlockId, i32>,
    pins: Vec<BlockId>,
    bm: Arc<BufferManager>,
}

impl BufferList {
    pub fn new(bm: Arc<BufferManager>) -> Self {
        Self {
            buffers: HashMap::new(),
            pins: Vec::new(),
            bm,
        }
    }

    pub fn buffer(&self, block: &BlockId) -> &Buffer {
        let i = self.buffers.get(block).unwrap();
        self.bm.get(*i)
    }

    pub fn buffer_mut(&mut self, block: &BlockId) -> &mut Buffer {
        let i = self.buffers.get(block).unwrap();
        let bm = Arc::as_ptr(&self.bm) as *mut BufferManager;
        unsafe { return (*bm).get_mut(*i) }
    }

    pub fn pin(&mut self, block: BlockId) -> Result<()> {
        let bm = Arc::as_ptr(&self.bm) as *mut BufferManager;
        let buffer = unsafe { (*bm).pin(&block)? };
        self.buffers.insert(block.clone(), buffer);
        self.pins.push(block);
        Ok(())
    }

    pub fn unpin(&mut self, block: BlockId) {
        let i = self.buffers.get(&block);
        if i.is_none() {
            return;
        }
        let i = *i.unwrap();

        let bm = Arc::as_ptr(&self.bm) as *mut BufferManager;
        unsafe {
            (*bm).unpin(i);
        }
        let pos = self.pins.iter().position(|b| b == &block).unwrap();
        self.pins.remove(pos);
        if !self.pins.contains(&block) {
            self.buffers.remove(&block);
        }
    }

    pub fn unpin_all(&mut self) {
        for block in &self.pins {
            let i = self.buffers.get(block).unwrap();
            let bm = Arc::as_ptr(&self.bm) as *mut BufferManager;
            unsafe {
                (*bm).unpin(*i);
            }
        }
        self.pins.clear();
        self.buffers.clear();
    }
}
