#![allow(dead_code)]

use std::{collections::HashMap, sync::LazyLock};

use crate::{file::block_id::BlockId, util::Result};

use super::lock_table::LockTable;

static mut LOCK_TABLE: LazyLock<LockTable> = LazyLock::new(LockTable::new);

pub struct ConcurrencyManager {
    locks: HashMap<BlockId, char>,
}

impl ConcurrencyManager {
    pub fn new() -> Self {
        Self {
            locks: HashMap::new(),
        }
    }

    pub fn s_lock(&mut self, block: &BlockId) -> Result<()> {
        if !self.locks.contains_key(block) {
            unsafe {
                let lock_table = &*LOCK_TABLE as *const LockTable as *mut LockTable;
                (*lock_table).s_lock(block)?;
            }
            self.locks.insert(block.clone(), 'S');
        }
        Ok(())
    }

    pub fn x_lock(&mut self, block: &BlockId) -> Result<()> {
        if !self.has_x_lock(block) {
            self.s_lock(block)?;
            unsafe {
                let lock_table = &*LOCK_TABLE as *const LockTable as *mut LockTable;
                (*lock_table).x_lock(block)?;
            }
            self.locks.insert(block.clone(), 'X');
        }
        Ok(())
    }

    pub fn release(&mut self) {
        for block in self.locks.keys() {
            unsafe {
                let lock_table = &*LOCK_TABLE as *const LockTable as *mut LockTable;
                (*lock_table).unlock(block);
            }
        }
        self.locks.clear();
    }

    fn has_x_lock(&self, block: &BlockId) -> bool {
        self.locks.get(block).map_or(false, |&lock| lock == 'X')
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn xlock_then_xlock() {
        let mut cm1 = ConcurrencyManager::new();
        let mut cm2 = ConcurrencyManager::new();
        let block = BlockId::new("file".to_string(), 0);

        cm1.x_lock(&block).unwrap();

        let res = cm2.x_lock(&block);
        assert!(res.is_err());
    }
}
