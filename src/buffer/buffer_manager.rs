#![allow(dead_code)]

use std::{
    collections::{BTreeSet, HashMap},
    sync::Arc,
};

use crate::{
    file::{block_id::BlockId, file_manager::FileManager},
    log::log_manager::LogManager,
    util::{current_time_millis, waiting_too_long, CondMutex, Result, MAX_WAIT_TIME_MILLIS},
};

use super::buffer::Buffer;

pub struct BufferManager {
    m: CondMutex<()>,
    buffer_pool: Vec<Buffer>,
    num_available: i32,
    unpinned_positions: BTreeSet<i32>,
    existing_positions: HashMap<BlockId, i32>,
}

impl BufferManager {
    pub fn new(fm: Arc<FileManager>, lm: Arc<LogManager>, num_buffers: i32) -> Self {
        let mut buffer_pool = Vec::new();
        for _ in 0..num_buffers {
            buffer_pool.push(Buffer::new(fm.clone(), lm.clone()));
        }
        BufferManager {
            m: CondMutex::new(()),
            buffer_pool,
            num_available: num_buffers,
            unpinned_positions: (0..num_buffers).collect(),
            existing_positions: HashMap::new(),
        }
    }

    pub fn get(&self, buf_idx: i32) -> &Buffer {
        &self.buffer_pool[buf_idx as usize]
    }

    pub fn get_mut(&mut self, buf_idx: i32) -> &mut Buffer {
        &mut self.buffer_pool[buf_idx as usize]
    }

    pub fn available(&self) -> i32 {
        let _lock = self.m.lock();
        self.num_available
    }

    pub fn flush_all(&mut self, tx_num: i32) -> Result<()> {
        let _lock = self.m.lock();
        for buffer in self.buffer_pool.iter_mut() {
            if buffer.modifying_tx() == tx_num || (tx_num == -1 && buffer.modifying_tx() != -1) {
                buffer.flush()?;
            }
        }
        Ok(())
    }

    pub fn unpin(&mut self, buf_idx: i32) {
        let _lock = self.m.lock();
        let buffer = &mut self.buffer_pool[buf_idx as usize];
        buffer.unpin();
        if !buffer.is_pinned() {
            self.num_available += 1;
            self.unpinned_positions.insert(buf_idx);
            self.m.notify_all();
        }
    }

    pub fn pin(&mut self, block: &BlockId) -> Result<i32> {
        let mut lock = self.m.lock();
        let start_time = current_time_millis();
        let buffer_pool_ptr = &mut self.buffer_pool as *mut Vec<Buffer>;
        loop {
            let buffer_pool = unsafe { &mut *buffer_pool_ptr };
            let buffer = Self::try_to_pin(
                buffer_pool,
                &mut self.num_available,
                block,
                &mut self.unpinned_positions,
                &mut self.existing_positions,
            )?;
            if buffer.is_some() || waiting_too_long(start_time) {
                return buffer.ok_or("no available buffer".into());
            }

            lock = self.m.wait_timeout(lock, MAX_WAIT_TIME_MILLIS as u64);
        }
    }

    fn try_to_pin(
        buffer_pool: &mut [Buffer],
        num_available: &mut i32,
        block: &BlockId,
        unpinned_positions: &mut BTreeSet<i32>,
        existing_positions: &mut HashMap<BlockId, i32>,
    ) -> Result<Option<i32>> {
        let existing_position = Self::existing_position(existing_positions, block);
        let unpinned_position = Self::unpinned_position(unpinned_positions);
        if existing_position.is_none() && unpinned_position.is_none() {
            return Ok(None);
        }

        let (buffer, position) = if existing_position.is_some() {
            let position = existing_position.unwrap();
            (&mut buffer_pool[position as usize], position)
        } else {
            let position = unpinned_position.unwrap();
            let buffer = &mut buffer_pool[position as usize];
            buffer.assign_to_block(block.clone())?;
            existing_positions.insert(block.clone(), position);
            (buffer, position)
        };
        if !buffer.is_pinned() {
            *num_available -= 1;
            unpinned_positions.remove(&position);
        }
        buffer.pin();
        Ok(Some(position))
    }

    fn existing_position(
        existing_positions: &HashMap<BlockId, i32>,
        block: &BlockId,
    ) -> Option<i32> {
        existing_positions.get(block).copied()
    }

    fn unpinned_position(unpinned_positions: &BTreeSet<i32>) -> Option<i32> {
        unpinned_positions.iter().next().copied()
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
        let lm = Arc::new(LogManager::new(fm.clone(), "templog".to_string()));
        let mut bm = BufferManager::new(fm.clone(), lm, 3);
        assert_eq!(bm.available(), 3);

        let mut buffers: Vec<i32> = vec![];

        // fill buffer
        for i in 0..3 {
            let buf = bm.pin(&BlockId::new("testfile".to_string(), i)).unwrap();
            assert_eq!(buf, i);
            assert_eq!(bm.available(), 2 - i);
            buffers.push(buf as i32);
        }

        // free
        bm.unpin(buffers[1]);
        assert_eq!(bm.available(), 1);
        buffers[1] = -1;

        // fill buffer
        for i in 0..2 {
            let buf = bm.pin(&BlockId::new("testfile".to_string(), i)).unwrap();
            assert_eq!(buf, i);
            assert_eq!(bm.available(), 1 - i);
            buffers.push(buf as i32);
        }

        // buffer is full
        let res = bm.pin(&BlockId::new("testfile".to_string(), 3));
        assert!(res.is_err());

        // free
        bm.unpin(buffers[2]);
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
        let lm = Arc::new(LogManager::new(fm.clone(), "templog".to_string()));
        let mut bm = BufferManager::new(fm.clone(), lm, 3);

        // 0: modify and set_modified
        bm.pin(&BlockId::new("testfile".to_string(), 0)).unwrap();
        let buf = bm.get_mut(0);
        buf.contents.set_string(0, "abcde");
        buf.set_modified(1, 1);

        // 1: modify and set_modified
        bm.pin(&BlockId::new("testfile".to_string(), 1)).unwrap();
        let buf = bm.get_mut(1);
        buf.contents.set_string(0, "fghij");
        buf.set_modified(1, 2);

        // 2: just modify, not set_modified
        bm.pin(&BlockId::new("testfile".to_string(), 2)).unwrap();
        let buf = bm.get_mut(2);
        buf.contents.set_string(0, "klmno");

        bm.flush_all(1).unwrap();

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
