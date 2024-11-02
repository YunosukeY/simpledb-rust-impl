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

const MAX_TIME: u128 = 1_000;

pub struct BufferManager {
    m: Mutex<()>,
    cond: Condvar,
    buffer_pool: Vec<Buffer>,
    num_available: i32,
}

impl BufferManager {
    pub fn new(fm: Arc<FileManager>, lm: LogManager, num_buffers: i32) -> Self {
        let mut buffer_pool = Vec::new();
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

    pub fn get(&self, buf_idx: usize) -> &Buffer {
        &self.buffer_pool[buf_idx]
    }

    pub fn get_mut(&mut self, buf_idx: usize) -> &mut Buffer {
        &mut self.buffer_pool[buf_idx]
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

    pub fn unpin(&mut self, buf_idx: usize) {
        let _lock = self.m.lock().unwrap();
        let buffer = &mut self.buffer_pool[buf_idx];
        buffer.unpin();
        if !buffer.is_pinned() {
            self.num_available += 1;
            self.cond.notify_all();
        }
    }

    pub fn pin(&mut self, block: &BlockId) -> Result<usize> {
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

    fn try_to_pin(
        buffer_pool: &mut Vec<Buffer>,
        num_available: &mut i32,
        block: &BlockId,
    ) -> Option<usize> {
        let existing_buffer_position = Self::existing_buffer_position(buffer_pool, &block);
        let unpinned_buffer_position = Self::unpinned_buffer_position(buffer_pool);
        if existing_buffer_position.is_none() && unpinned_buffer_position.is_none() {
            return None;
        }

        let (buffer, position) = if existing_buffer_position.is_some() {
            let position = existing_buffer_position.unwrap();
            (&mut buffer_pool[position], position)
        } else {
            let position = unpinned_buffer_position.unwrap();
            let buffer = &mut buffer_pool[position];
            buffer.assign_to_block(block.clone()).unwrap();
            (buffer, position)
        };
        if !buffer.is_pinned() {
            *num_available -= 1;
        }
        buffer.pin();
        Some(position)
    }

    fn existing_buffer_position(buffer_pool: &Vec<Buffer>, block: &BlockId) -> Option<usize> {
        buffer_pool
            .iter()
            .position(|buffer| buffer.block().as_ref() == Some(block))
    }

    fn unpinned_buffer_position(buffer_pool: &Vec<Buffer>) -> Option<usize> {
        buffer_pool.iter().position(|buffer| !buffer.is_pinned())
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    #[test]
    fn pin_and_unpin() {
        // create testfile
        std::fs::write(
            "testdata/buffer/buffer_manager/pin_and_unpin/testfile",
            "a".to_string().repeat(40),
        )
        .unwrap();

        let fm = FileManager::new(
            PathBuf::from("testdata/buffer/buffer_manager/pin_and_unpin"),
            10,
        );
        let fm = Arc::new(fm);
        let lm = LogManager::new(fm.clone(), "templog".to_string());
        let mut bm = BufferManager::new(fm.clone(), lm, 3);
        assert_eq!(bm.available(), 3);

        let mut buffers: Vec<i32> = vec![];

        // fill buffer
        for i in 0..3 {
            let buf = bm.pin(&BlockId::new("testfile".to_string(), i)).unwrap();
            assert_eq!(buf, i as usize);
            assert_eq!(bm.available(), 2 - i);
            buffers.push(buf as i32);
        }

        // free
        bm.unpin(buffers[1] as usize);
        assert_eq!(bm.available(), 1);
        buffers[1] = -1;

        // fill buffer
        for i in 0..2 {
            let buf = bm.pin(&BlockId::new("testfile".to_string(), i)).unwrap();
            assert_eq!(buf, i as usize);
            assert_eq!(bm.available(), 1 - i);
            buffers.push(buf as i32);
        }

        // buffer is full
        let res = bm.pin(&BlockId::new("testfile".to_string(), 3));
        assert!(res.is_err());

        // free
        bm.unpin(buffers[2] as usize);
        assert_eq!(bm.available(), 1);
        buffers[2] = -1;

        // now buffer is available
        let buf = bm.pin(&BlockId::new("testfile".to_string(), 3)).unwrap();
        assert_eq!(buf, 2);
        assert_eq!(bm.available(), 0);
        buffers.push(buf as i32);

        // delete testfile
        std::fs::remove_file("testdata/buffer/buffer_manager/pin_and_unpin/testfile").unwrap();
    }

    #[test]
    fn modify_and_flush() {
        // create testfile
        std::fs::write(
            "testdata/buffer/buffer_manager/modify_and_flush/testfile",
            "\0".to_string().repeat(30),
        )
        .unwrap();

        let fm = FileManager::new(
            PathBuf::from("testdata/buffer/buffer_manager/modify_and_flush"),
            10,
        );
        let fm = Arc::new(fm);
        let lm = LogManager::new(fm.clone(), "templog".to_string());
        let mut bm = BufferManager::new(fm.clone(), lm, 3);

        // 0: modify and set_modified
        bm.pin(&BlockId::new("testfile".to_string(), 0)).unwrap();
        let buf = bm.get_mut(0);
        buf.contents().set_string(0, "abcde");
        buf.set_modified(1, 1);

        // 1: modify and set_modified
        bm.pin(&BlockId::new("testfile".to_string(), 1)).unwrap();
        let buf = bm.get_mut(1);
        buf.contents().set_string(0, "fghij");
        buf.set_modified(1, 2);

        // 2: just modify, not set_modified
        bm.pin(&BlockId::new("testfile".to_string(), 2)).unwrap();
        let buf = bm.get_mut(2);
        buf.contents().set_string(0, "klmno");

        bm.flush_all(1);

        // 0 and 1 are flushed, 2 is not
        assert_eq!(
            std::fs::read_to_string("testdata/buffer/buffer_manager/modify_and_flush/testfile")
                .unwrap(),
            "\0\0\0\u{5}abcde\0\0\0\0\u{5}fghij\0\0\0\0\0\0\0\0\0\0\0"
        );

        // delete testfile
        std::fs::remove_file("testdata/buffer/buffer_manager/modify_and_flush/testfile").unwrap();
    }
}
