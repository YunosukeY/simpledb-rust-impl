#![allow(dead_code)]
#![allow(unused_variables)]

use std::sync::{Arc, Mutex};

use crate::{
    buffer::buffer_manager::BufferManager,
    file::{block_id::BlockId, file_manager::FileManager},
    log::log_manager::LogManager,
};

use super::{
    buffer_list::BufferList, concurrency::concurrency_manager::ConcurrencyManager,
    recovery::recovery_manager::RecoveryManager,
};

static NEXT_TX_NUM: Mutex<i32> = Mutex::new(0);
const END_OF_FILE: i32 = -1;

pub struct Transaction {
    rm: RecoveryManager,
    cm: ConcurrencyManager,
    bm: Arc<BufferManager>,
    fm: FileManager,
    tx_num: i32,
    my_buffers: BufferList,
}

impl Transaction {
    pub fn new(fm: FileManager, lm: LogManager, bm: BufferManager) -> Self {
        let tx_num = Self::next_tx_number();
        let bm = Arc::new(bm);
        let rm = RecoveryManager::new(tx_num, lm, bm.clone());
        let cm = ConcurrencyManager::new();
        let my_buffers = BufferList::new(bm.clone());
        Self {
            rm,
            cm,
            bm,
            fm,
            tx_num,
            my_buffers,
        }
    }

    pub fn commit(&mut self) {
        self.rm.commit();
        // TODO log
        self.cm.release();
        self.my_buffers.unpin_all();
    }

    pub fn rollback(&mut self) {
        let rm = &mut self.rm as *mut RecoveryManager;
        unsafe {
            (*rm).rollback(self);
        }
        // TODO log
        self.cm.release();
        self.my_buffers.unpin_all();
    }

    pub fn recover(&mut self) {
        let bm = Arc::as_ptr(&self.bm) as *mut BufferManager;
        unsafe {
            (*bm).flush_all(self.tx_num).unwrap();
        }
        let rm = &mut self.rm as *mut RecoveryManager;
        unsafe {
            (*rm).recover(self);
        }
    }

    pub fn pin(&mut self, block: &BlockId) {
        self.my_buffers.pin(block.clone()).unwrap();
    }

    pub fn unpin(&mut self, block: &BlockId) {
        self.my_buffers.unpin(block.clone());
    }

    pub fn get_int(&mut self, block: &BlockId, offset: i32) -> i32 {
        self.cm.s_lock(block);
        let buffer = self.my_buffers.buffer(block).unwrap();
        buffer.contents.get_int(offset)
    }

    pub fn set_int(&mut self, block: &BlockId, offset: i32, value: i32, log: bool) {
        self.cm.x_lock(block.clone());
        let buffer = self.my_buffers.buffer_mut(block).unwrap();
        let mut lsn = -1;
        if log {
            lsn = self.rm.set_int(buffer, offset, value);
        }
        buffer.contents.set_int(offset, value);
        buffer.set_modified(self.tx_num, lsn);
    }

    pub fn get_bytes(&mut self, block: &BlockId, offset: i32) -> &[u8] {
        self.cm.s_lock(block);
        let buffer = self.my_buffers.buffer(block).unwrap();
        buffer.contents.get_bytes(offset)
    }

    pub fn set_bytes(&mut self, block: &BlockId, offset: i32, value: &[u8], log: bool) {
        self.cm.x_lock(block.clone());
        let buffer = self.my_buffers.buffer_mut(block).unwrap();
        let mut lsn = -1;
        if log {
            lsn = self.rm.set_bytes(buffer, offset, value);
        }
        buffer.contents.set_bytes(offset, value);
        buffer.set_modified(self.tx_num, lsn);
    }

    pub fn get_string(&mut self, block: &BlockId, offset: i32) -> String {
        self.cm.s_lock(block);
        let buffer = self.my_buffers.buffer(block).unwrap();
        buffer.contents.get_string(offset)
    }

    pub fn set_string(&mut self, block: &BlockId, offset: i32, value: &str, log: bool) {
        self.cm.x_lock(block.clone());
        let buffer = self.my_buffers.buffer_mut(block).unwrap();
        let mut lsn = -1;
        if log {
            lsn = self.rm.set_string(buffer, offset, value);
        }
        buffer.contents.set_string(offset, value);
        buffer.set_modified(self.tx_num, lsn);
    }

    pub fn get_bool(&mut self, block: &BlockId, offset: i32) -> bool {
        self.cm.s_lock(block);
        let buffer = self.my_buffers.buffer(block).unwrap();
        buffer.contents.get_bool(offset)
    }

    pub fn set_bool(&mut self, block: &BlockId, offset: i32, value: bool, log: bool) {
        self.cm.x_lock(block.clone());
        let buffer = self.my_buffers.buffer_mut(block).unwrap();
        let mut lsn = -1;
        if log {
            lsn = self.rm.set_bool(buffer, offset, value);
        }
        buffer.contents.set_bool(offset, value);
        buffer.set_modified(self.tx_num, lsn);
    }

    pub fn get_double(&mut self, block: &BlockId, offset: i32) -> f64 {
        self.cm.s_lock(block);
        let buffer = self.my_buffers.buffer(block).unwrap();
        buffer.contents.get_double(offset)
    }

    pub fn set_double(&mut self, block: &BlockId, offset: i32, value: f64, log: bool) {
        self.cm.x_lock(block.clone());
        let buffer = self.my_buffers.buffer_mut(block).unwrap();
        let mut lsn = -1;
        if log {
            lsn = self.rm.set_double(buffer, offset, value);
        }
        buffer.contents.set_double(offset, value);
        buffer.set_modified(self.tx_num, lsn);
    }

    pub fn get_date(&mut self, block: &BlockId, offset: i32) -> chrono::NaiveDate {
        self.cm.s_lock(block);
        let buffer = self.my_buffers.buffer(block).unwrap();
        buffer.contents.get_date(offset)
    }

    pub fn set_date(&mut self, block: &BlockId, offset: i32, value: &chrono::NaiveDate, log: bool) {
        self.cm.x_lock(block.clone());
        let buffer = self.my_buffers.buffer_mut(block).unwrap();
        let mut lsn = -1;
        if log {
            lsn = self.rm.set_date(buffer, offset, value);
        }
        buffer.contents.set_date(offset, value);
        buffer.set_modified(self.tx_num, lsn);
    }

    pub fn size(&mut self, filename: &str) -> i32 {
        let dummy = BlockId::new(filename.to_string(), END_OF_FILE);
        self.cm.x_lock(dummy);
        self.fm.length(filename).unwrap()
    }

    pub fn append(&mut self, filename: &str) -> BlockId {
        let dummy = BlockId::new(filename.to_string(), END_OF_FILE);
        self.cm.x_lock(dummy);
        self.fm.append(filename).unwrap()
    }

    pub fn block_size(&self) -> i32 {
        self.fm.block_size()
    }

    pub fn available(&self) -> i32 {
        self.bm.available()
    }

    fn next_tx_number() -> i32 {
        let mut next_tx_num = NEXT_TX_NUM.lock().unwrap();
        *next_tx_num += 1;
        *next_tx_num
    }
}
