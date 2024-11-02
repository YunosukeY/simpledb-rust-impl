use std::{
    sync::{Arc, Condvar, Mutex},
    time::Duration,
};

use crate::{
    file::{block_id::BlockId, file_manager::FileManager},
    log::log_manager::LogManager,
    util::Result,
};

use super::buffer::Buffer;

const MAX_TIME: u128 = 10_000;

pub struct BufferManager {
    m: Mutex<()>,
    cond: Condvar,
    buffer_pool: Vec<Buffer>,
    num_available: i32,
}

impl BufferManager {
    pub fn new(fm: FileManager, lm: LogManager, num_buffers: i32) -> Self {
        let mut buffer_pool = Vec::new();
        let fm = Arc::new(fm);
        let lm = Arc::new(lm);
        for _ in 0..num_buffers {
            buffer_pool.push(Buffer::new(fm.clone(), lm.clone()));
        }
        BufferManager {
            m: Mutex::new(()),
            cond: Condvar::new(),
            buffer_pool,
            num_available: num_buffers,
        }
    }

    pub fn available(&self) -> i32 {
        let _lock = self.m.lock().unwrap();
        self.num_available
    }

    pub fn flush_all(&mut self, tx_num: i32) {
        let _lock = self.m.lock().unwrap();
        for buffer in self.buffer_pool.iter_mut() {
            if buffer.modifying_tx() == tx_num {
                buffer.flush().unwrap();
            }
        }
    }

    pub fn unpin(&mut self, buffer: &mut Buffer) {
        let _lock = self.m.lock().unwrap();
        buffer.unpin();
        if !buffer.is_pinned() {
            self.num_available += 1;
            self.cond.notify_all();
        }
    }

    pub fn pin(&mut self, block: &BlockId) -> Result<&Buffer> {
        let mut lock = self.m.lock().unwrap();
        let start_time = Self::current_time_millis();
        let buffer_pool_ptr = &mut self.buffer_pool as *mut Vec<Buffer>;
        let buffer = loop {
            let buffer_pool = unsafe { &mut *buffer_pool_ptr };
            let buffer = Self::try_to_pin(buffer_pool, &mut self.num_available, &block);
            if buffer.is_some() || Self::waiting_too_long(start_time) {
                break buffer;
            }

            lock = self
                .cond
                .wait_timeout(lock, Duration::from_millis(MAX_TIME as u64))
                .unwrap()
                .0;
        };
        if buffer.is_none() {
            return Err("no available buffer".into());
        }
        Ok(buffer.unwrap())
    }

    fn current_time_millis() -> u128 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
    }

    fn waiting_too_long(start_time: u128) -> bool {
        let current_time = Self::current_time_millis();
        current_time - start_time > MAX_TIME
    }

    fn try_to_pin<'a, 'b>(
        buffer_pool: &'a mut Vec<Buffer>,
        num_available: &mut i32,
        block: &'b BlockId,
    ) -> Option<&'a mut Buffer> {
        let is_buffer_exist = Self::is_buffer_exist(buffer_pool, &block);
        let is_unpinned_buffer_exist = Self::is_unpinned_buffer_exist(buffer_pool);
        if !is_buffer_exist && !is_unpinned_buffer_exist {
            return None;
        }

        let buffer = if is_buffer_exist {
            Self::find_existing_buffer(buffer_pool, &block).unwrap()
        } else {
            Self::choose_unpinned_buffer(buffer_pool).unwrap()
        };
        if !buffer.is_pinned() {
            *num_available -= 1;
        }
        buffer.pin();
        Some(buffer)
    }

    fn is_buffer_exist(buffer_pool: &Vec<Buffer>, block: &BlockId) -> bool {
        buffer_pool
            .iter()
            .any(|buffer| buffer.block().as_ref() == Some(block))
    }

    fn find_existing_buffer<'a, 'b>(
        buffer_pool: &'a mut Vec<Buffer>,
        block: &'b BlockId,
    ) -> Option<&'a mut Buffer> {
        buffer_pool
            .iter_mut()
            .find(|buffer| buffer.block().as_ref() == Some(block))
    }

    fn is_unpinned_buffer_exist(buffer_pool: &Vec<Buffer>) -> bool {
        buffer_pool.iter().any(|buffer| !buffer.is_pinned())
    }

    fn choose_unpinned_buffer(buffer_pool: &mut Vec<Buffer>) -> Option<&mut Buffer> {
        buffer_pool.iter_mut().find(|buffer| !buffer.is_pinned())
    }
}
