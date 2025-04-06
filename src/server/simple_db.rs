#![allow(dead_code)]

use std::{path::PathBuf, sync::Arc};

use tracing::info;

use crate::{
    buffer::buffer_manager::BufferManager,
    file::file_manager::FileManager,
    log::log_manager::LogManager,
    metadata::metadata_manager::MetadataManager,
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
    mm: Option<MetadataManager>,
}

impl SimpleDB {
    /// only for tests
    pub fn _new(dir_name: &str, block_size: i32, buffer_size: i32, log_file: &str) -> Self {
        let fm = Arc::new(FileManager::new(PathBuf::from(dir_name), block_size));
        let lm = Arc::new(LogManager::new(fm.clone(), log_file.to_string()));
        let bm = Arc::new(BufferManager::new(fm.clone(), lm.clone(), buffer_size));
        let lock_table = Arc::new(LockTable::new());
        Self {
            fm,
            lm,
            bm,
            lock_table,
            mm: None,
        }
    }

    pub fn new(dir_name: &str) -> Self {
        let mut db = Self::_new(dir_name, BLOCK_SIZE, BUFFER_SIZE, LOG_FILE);
        let tx = Arc::new(db.new_tx());
        let is_new = db.fm.is_new();
        if is_new {
            info!("creating new database");
        } else {
            info!("recovering existing database");
            let tx_ptr = Arc::as_ptr(&tx) as *mut Transaction;
            unsafe {
                (*tx_ptr).recover();
            }
        }
        let mm = MetadataManager::new(is_new, tx.clone());
        let tx_ptr = Arc::as_ptr(&tx) as *mut Transaction;
        unsafe {
            (*tx_ptr).recover();
        }
        drop(tx);

        db.mm = Some(mm);
        db
    }

    pub fn new_tx(&self) -> Transaction {
        Transaction::new(
            self.fm.clone(),
            self.lm.clone(),
            self.bm.clone(),
            self.lock_table.clone(),
        )
    }

    /// only for tests
    pub fn _file_manager(&self) -> Arc<FileManager> {
        self.fm.clone()
    }

    /// only for tests
    pub fn _log_manager(&self) -> Arc<LogManager> {
        self.lm.clone()
    }

    /// only for tests
    pub fn _buffer_manager(&self) -> Arc<BufferManager> {
        self.bm.clone()
    }
}
