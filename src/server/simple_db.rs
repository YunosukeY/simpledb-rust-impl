#![allow(dead_code)]

use std::{path::PathBuf, sync::Arc};

use crate::{
    buffer::buffer_manager::BufferManager,
    file::file_manager::FileManager,
    log::log_manager::LogManager,
    tx::{concurrency::lock_table::LockTable, transaction::Transaction},
};

pub const BLOCK_SIZE: i32 = 400;
pub const BUFFER_SIZE: i32 = 8;
pub const LOG_FILE: &str = "simpledb.log";

#[derive(Clone)]
pub struct SimpleDB {
    fm: Arc<FileManager>,
    lm: Arc<LogManager>,
    bm: Arc<BufferManager>,
    lock_table: Arc<LockTable>,
}

impl SimpleDB {
    pub fn new(dir_name: &str, block_size: i32, buffer_size: i32, log_file: &str) -> Self {
        let fm = Arc::new(FileManager::new(PathBuf::from(dir_name), block_size));
        let lm = Arc::new(LogManager::new(fm.clone(), log_file.to_string()));
        let bm = Arc::new(BufferManager::new(fm.clone(), lm.clone(), buffer_size));
        let lock_table = Arc::new(LockTable::new());
        Self {
            fm,
            lm,
            bm,
            lock_table,
        }
    }

    pub fn new_tx(&self) -> Transaction {
        Transaction::new(
            self.fm.clone(),
            self.lm.clone(),
            self.bm.clone(),
            self.lock_table.clone(),
        )
    }

    pub fn file_manager(&self) -> Arc<FileManager> {
        self.fm.clone()
    }

    pub fn log_manager(&self) -> Arc<LogManager> {
        self.lm.clone()
    }

    pub fn buffer_manager(&self) -> Arc<BufferManager> {
        self.bm.clone()
    }
}
