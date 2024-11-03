#![allow(dead_code)]

use std::{
    collections::HashMap,
    sync::{Condvar, Mutex},
    time::Duration,
};

use crate::{
    file::block_id::BlockId,
    util::{current_time_millis, waiting_too_long, Result, MAX_WAIT_TIME_MILLIS},
};

pub(super) struct LockTable {
    m: Mutex<()>,
    cond: Condvar,
    locks: HashMap<BlockId, i32>,
}

impl LockTable {
    pub fn new() -> Self {
        Self {
            m: Mutex::new(()),
            cond: Condvar::new(),
            locks: HashMap::new(),
        }
    }

    pub fn s_lock(&mut self, block: &BlockId) -> Result<()> {
        let mut lock = self.m.lock().unwrap();
        let start_time = current_time_millis();
        while Self::has_x_lock(&self.locks, block) && !waiting_too_long(start_time) {
            lock = self
                .cond
                .wait_timeout(lock, Duration::from_millis(MAX_WAIT_TIME_MILLIS as u64))
                .unwrap()
                .0;
        }
        if Self::has_x_lock(&self.locks, block) {
            return Err("deadlock".into());
        }
        let value = Self::lock_value(&self.locks, block);
        self.locks.insert(block.clone(), value + 1);
        Ok(())
    }

    pub fn x_lock(&mut self, block: &BlockId) -> Result<()> {
        let mut lock = self.m.lock().unwrap();
        let start_time = current_time_millis();
        while Self::has_other_s_locks(&self.locks, block) && !waiting_too_long(start_time) {
            lock = self
                .cond
                .wait_timeout(lock, Duration::from_millis(MAX_WAIT_TIME_MILLIS as u64))
                .unwrap()
                .0;
        }
        if Self::has_other_s_locks(&self.locks, block) {
            return Err("deadlock".into());
        }
        self.locks.insert(block.clone(), -1);

        Ok(())
    }

    pub fn unlock(&mut self, block: &BlockId) {
        let _lock = self.m.lock().unwrap();
        let value = Self::lock_value(&self.locks, block);
        if value > 1 {
            self.locks.insert(block.clone(), value - 1);
        } else {
            self.locks.remove(block);
            self.cond.notify_all();
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
