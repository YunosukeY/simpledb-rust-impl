#![allow(dead_code)]

use std::collections::HashMap;

use crate::{
    buffer::{buffer::Buffer, buffer_manager::BufferManager},
    file::block_id::BlockId,
    util::Result,
};

pub(super) struct BufferList {
    buffers: HashMap<BlockId, i32>,
    pins: Vec<BlockId>,
    bm: BufferManager,
}

impl BufferList {
    pub fn new(bm: BufferManager) -> Self {
        Self {
            buffers: HashMap::new(),
            pins: Vec::new(),
            bm,
        }
    }

    pub fn buffer(&self, block: BlockId) -> Option<&Buffer> {
        let i = self.buffers.get(&block);
        match i {
            Some(i) => Some(self.bm.get(*i)),
            None => None,
        }
    }

    pub fn pin(&mut self, block: BlockId) -> Result<()> {
        let buffer = self.bm.pin(&block)?;
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

        self.bm.unpin(i);
        let pos = self.pins.iter().position(|b| b == &block).unwrap();
        self.pins.remove(pos);
        if !self.pins.contains(&block) {
            self.buffers.remove(&block);
        }
    }

    pub fn unpin_all(&mut self) {
        for block in &self.pins {
            let i = self.buffers.get(block).unwrap();
            self.bm.unpin(*i);
        }
        self.pins.clear();
        self.buffers.clear();
    }
}
