#![allow(dead_code)]

use std::{collections::HashMap, sync::Arc};

use crate::{file::block_id::BlockId, util::Result};

use super::lock_table::LockTable;

pub struct ConcurrencyManager {
    locks: HashMap<BlockId, char>,
    lock_table: Arc<LockTable>,
}

impl ConcurrencyManager {
    pub fn new(lock_table: Arc<LockTable>) -> Self {
        Self {
            locks: HashMap::new(),
            lock_table,
        }
    }

    pub fn s_lock(&mut self, block: &BlockId) -> Result<()> {
        if !self.locks.contains_key(block) {
            let lock_table = Arc::as_ptr(&self.lock_table) as *mut LockTable;
            unsafe {
                (*lock_table).s_lock(block)?;
            }
            self.locks.insert(block.clone(), 'S');
        }
        Ok(())
    }

    pub fn x_lock(&mut self, block: &BlockId) -> Result<()> {
        if !self.has_x_lock(block) {
            self.s_lock(block)?;
            let lock_table = Arc::as_ptr(&self.lock_table) as *mut LockTable;
            unsafe {
                (*lock_table).x_lock(block)?;
            }
            self.locks.insert(block.clone(), 'X');
        }
        Ok(())
    }

    pub fn release(&mut self) {
        for block in self.locks.keys() {
            let lock_table = Arc::as_ptr(&self.lock_table) as *mut LockTable;
            unsafe {
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
        let lock_table = Arc::new(LockTable::new());
        let mut cm1 = ConcurrencyManager::new(lock_table.clone());
        let mut cm2 = ConcurrencyManager::new(lock_table.clone());
        let block = BlockId::new("file".to_string(), 0);

        cm1.x_lock(&block).unwrap();

        let res = cm2.x_lock(&block);
        assert!(res.is_err());
    }
}
