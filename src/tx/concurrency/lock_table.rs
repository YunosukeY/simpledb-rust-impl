#![allow(dead_code)]

use std::collections::HashMap;

use crate::{
    file::block_id::BlockId,
    util::{
        current_time_millis, waiting_too_long, ConcurrentHashMap, CondMutex, Result,
        MAX_WAIT_TIME_MILLIS,
    },
};

pub struct LockTable {
    ms: ConcurrentHashMap<BlockId, CondMutex<()>>,
    locks: HashMap<BlockId, i32>,
}

impl LockTable {
    pub fn new() -> Self {
        Self {
            ms: ConcurrentHashMap::new(),
            locks: HashMap::new(),
        }
    }

    pub(super) fn s_lock(&mut self, block: &BlockId) -> Result<()> {
        let m = self.ms.get_or_insert(block, CondMutex::new(()));
        let mut lock = m.lock();
        let start_time = current_time_millis();
        while Self::has_x_lock(&self.locks, block) && !waiting_too_long(start_time) {
            lock = m.wait_timeout(lock, MAX_WAIT_TIME_MILLIS as u64)
        }
        if Self::has_x_lock(&self.locks, block) {
            return Err("deadlock".into());
        }
        let value = Self::lock_value(&self.locks, block);
        self.locks.insert(block.clone(), value + 1);
        Ok(())
    }

    pub(super) fn x_lock(&mut self, block: &BlockId) -> Result<()> {
        let m = self.ms.get_or_insert(block, CondMutex::new(()));
        let mut lock = m.lock();
        let start_time = current_time_millis();
        while Self::has_other_s_locks(&self.locks, block) && !waiting_too_long(start_time) {
            lock = m.wait_timeout(lock, MAX_WAIT_TIME_MILLIS as u64);
        }
        if Self::has_other_s_locks(&self.locks, block) {
            return Err("deadlock".into());
        }
        self.locks.insert(block.clone(), -1);

        Ok(())
    }

    pub(super) fn unlock(&mut self, block: &BlockId) {
        let m = self.ms.get_or_insert(block, CondMutex::new(()));
        let _lock = m.lock();
        let value = Self::lock_value(&self.locks, block);
        if value > 1 {
            self.locks.insert(block.clone(), value - 1);
        } else {
            self.locks.remove(block);
            m.notify_all();
        }
    }

    fn has_x_lock(locks: &HashMap<BlockId, i32>, block: &BlockId) -> bool {
        Self::lock_value(locks, block) < 0
    }

    fn has_other_s_locks(locks: &HashMap<BlockId, i32>, block: &BlockId) -> bool {
        Self::lock_value(locks, block) > 1
    }

    fn lock_value(locks: &HashMap<BlockId, i32>, block: &BlockId) -> i32 {
        locks.get(block).map_or(0, |v| *v)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn slock_then_slock() {
        let mut lock_table = LockTable::new();
        let block = BlockId::new("file".to_string(), 0);

        lock_table.s_lock(&block).unwrap();

        let res = lock_table.s_lock(&block);
        assert!(res.is_ok());
    }

    #[test]
    fn slock_then_xlock() {
        let mut lock_table = LockTable::new();
        let block = BlockId::new("file".to_string(), 0);

        lock_table.x_lock(&block).unwrap();

        let res = lock_table.s_lock(&block);
        assert!(res.is_err());
    }

    #[test]
    fn xlock_then_slock() {
        let mut lock_table = LockTable::new();
        let block = BlockId::new("file".to_string(), 0);

        lock_table.x_lock(&block).unwrap();

        let res = lock_table.s_lock(&block);
        assert!(res.is_err());
    }

    #[test]
    fn xlock_then_xlock() {
        let mut lock_table = LockTable::new();
        let block = BlockId::new("file".to_string(), 0);

        lock_table.x_lock(&block).unwrap();

        let res = lock_table.x_lock(&block);
        assert!(res.is_ok()); // x_lock同士の競合はConcurrencyManagerレベルで解決される
    }

    #[test]
    fn slock_then_unlock() {
        let mut lock_table = LockTable::new();
        let block = BlockId::new("file".to_string(), 0);

        lock_table.s_lock(&block).unwrap();
        assert_eq!(LockTable::lock_value(&lock_table.locks, &block), 1);

        lock_table.unlock(&block);

        assert_eq!(LockTable::lock_value(&lock_table.locks, &block), 0);
    }

    #[test]
    fn xlock_then_unlock() {
        let mut lock_table = LockTable::new();
        let block = BlockId::new("file".to_string(), 0);

        lock_table.x_lock(&block).unwrap();
        assert_eq!(LockTable::lock_value(&lock_table.locks, &block), -1);

        lock_table.unlock(&block);
        assert_eq!(LockTable::lock_value(&lock_table.locks, &block), 0);
    }
}
