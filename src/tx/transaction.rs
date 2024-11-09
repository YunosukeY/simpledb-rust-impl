#![allow(dead_code)]
#![allow(unused_variables)]

use std::{
    sync::{Arc, Mutex, RwLock, RwLockReadGuard},
    thread,
    time::Duration,
};

use tracing::info;

use crate::{
    buffer::buffer_manager::BufferManager,
    file::{block_id::BlockId, file_manager::FileManager},
    log::log_manager::LogManager,
    util::{current_time_millis, waiting_too_long, Result},
};

use super::{
    buffer_list::BufferList,
    concurrency::{concurrency_manager::ConcurrencyManager, lock_table::LockTable},
    recovery::{
        checkpoint_record::CheckpointRecord, nq_ckpt_record::NqCkptRecord,
        recovery_manager::RecoveryManager,
    },
};

static NEXT_TX_NUM: Mutex<i32> = Mutex::new(0);
static CHECKPOINT_LOCK: Mutex<()> = Mutex::new(());
static TRANSACTION_LOCK: RwLock<()> = RwLock::new(());
static TRANSACTIONS: Mutex<Vec<i32>> = Mutex::new(Vec::new());
const END_OF_FILE: i32 = -1;

pub struct Transaction<'a> {
    rm: RecoveryManager,
    cm: ConcurrencyManager,
    bm: Arc<BufferManager>,
    fm: Arc<FileManager>,
    tx_num: i32,
    my_buffers: BufferList,
    tx_lock: RwLockReadGuard<'a, ()>,
}

impl<'a> Transaction<'a> {
    pub fn new(
        fm: Arc<FileManager>,
        lm: Arc<LogManager>,
        bm: Arc<BufferManager>,
        lock_table: Arc<LockTable>,
    ) -> Self {
        let tx_num = Self::next_tx_number();
        let tx_lock = {
            // Wait if a checkpoint is in progress.
            let _cp_lock = CHECKPOINT_LOCK.lock().unwrap();
            // Mark that some transactions are in progress.
            TRANSACTIONS.lock().unwrap().push(tx_num);
            TRANSACTION_LOCK.read().unwrap()
        };
        let rm = RecoveryManager::new(tx_num, lm, bm.clone());
        let cm = ConcurrencyManager::new(lock_table);
        let my_buffers = BufferList::new(bm.clone());
        Self {
            rm,
            cm,
            bm,
            fm,
            tx_num,
            my_buffers,
            tx_lock,
        }
    }

    pub fn commit(mut self) -> Result<()> {
        self.rm.commit()?;
        info!(self.tx_num, "transaction committed");
        self.cm.release();
        self.my_buffers.unpin_all();
        TRANSACTIONS.lock().unwrap().retain(|&x| x != self.tx_num);
        Ok(())
    }

    pub fn checkpoint(bm: Arc<BufferManager>, lm: Arc<LogManager>) -> Result<()> {
        // Stop accepting new transactions.
        let _cp_lock = CHECKPOINT_LOCK.lock().unwrap();

        // Wait for existing transactions to finish.
        let start_time = current_time_millis();
        loop {
            let tx_lock = TRANSACTION_LOCK.try_write();
            if tx_lock.is_ok() {
                break;
            }
            thread::sleep(Duration::from_millis(10));
            if waiting_too_long(start_time) {
                return Err("checkpoint timeout".into());
            }
        }

        // Flush all modified buffers.
        let bm = Arc::as_ptr(&bm) as *mut BufferManager;
        unsafe {
            (*bm).flush_all(-1)?;
        }

        // Append a quiescent checkpoint record to the log and flush it to disk.
        let lm = Arc::as_ptr(&lm) as *mut LogManager;
        unsafe {
            let lsn = CheckpointRecord::new().write_to_log(&mut *lm)?;
            (*lm).flush(lsn)?;
        }

        Ok(())
    }

    // Nonquiescent Checkpointing
    pub fn nq_ckpt(bm: Arc<BufferManager>, lm: Arc<LogManager>) -> Result<()> {
        // Stop accepting new transactions.
        let _cp_lock = CHECKPOINT_LOCK.lock().unwrap();

        // Flush all modified buffers.
        let bm = Arc::as_ptr(&bm) as *mut BufferManager;
        unsafe {
            (*bm).flush_all(-1)?;
        }

        // Write the record <NQCKPT T1 k> into the log.
        let tx_nums = TRANSACTIONS.lock().unwrap().clone();
        let lm = Arc::as_ptr(&lm) as *mut LogManager;
        unsafe {
            let lsn = NqCkptRecord::new(tx_nums).write_to_log(&mut *lm)?;
            (*lm).flush(lsn)?;
        }

        Ok(())
    }

    pub fn rollback(mut self) {
        let rm = &mut self.rm as *mut RecoveryManager;
        unsafe {
            (*rm).rollback(&mut self);
        }
        info!(self.tx_num, "transaction rolled back");
        self.cm.release();
        self.my_buffers.unpin_all();
        TRANSACTIONS.lock().unwrap().retain(|&x| x != self.tx_num);
    }

    pub fn recover(mut self) {
        let bm = Arc::as_ptr(&self.bm) as *mut BufferManager;
        unsafe {
            (*bm).flush_all(self.tx_num).unwrap();
        }
        let rm = &mut self.rm as *mut RecoveryManager;
        unsafe {
            (*rm).recover(&mut self);
        }
        self.cm.release();
        self.my_buffers.unpin_all();
        TRANSACTIONS.lock().unwrap().retain(|&x| x != self.tx_num);
    }

    pub fn pin(&mut self, block: &BlockId) -> Result<()> {
        self.my_buffers.pin(block.clone())
    }

    pub fn unpin(&mut self, block: &BlockId) {
        self.my_buffers.unpin(block.clone());
    }

    pub fn get_int(&mut self, block: &BlockId, offset: i32) -> Result<i32> {
        self.cm.s_lock(block)?;
        let buffer = self.my_buffers.buffer(block);
        Ok(buffer.contents.get_int(offset))
    }
    pub fn set_int(&mut self, block: &BlockId, offset: i32, value: i32, log: bool) -> Result<()> {
        self.cm.x_lock(block)?;
        let buffer = self.my_buffers.buffer_mut(block);
        let mut lsn = -1;
        if log {
            lsn = self.rm.set_int(buffer, offset, value)?;
        }
        buffer.contents.set_int(offset, value);
        buffer.set_modified(self.tx_num, lsn);
        Ok(())
    }

    pub fn get_bytes(&mut self, block: &BlockId, offset: i32) -> Result<&[u8]> {
        self.cm.s_lock(block)?;
        let buffer = self.my_buffers.buffer(block);
        Ok(buffer.contents.get_bytes(offset))
    }
    pub fn set_bytes(
        &mut self,
        block: &BlockId,
        offset: i32,
        value: &[u8],
        log: bool,
    ) -> Result<()> {
        self.cm.x_lock(block)?;
        let buffer = self.my_buffers.buffer_mut(block);
        let mut lsn = -1;
        if log {
            lsn = self.rm.set_bytes(buffer, offset, value)?;
        }
        buffer.contents.set_bytes(offset, value);
        buffer.set_modified(self.tx_num, lsn);
        Ok(())
    }

    pub fn get_string(&mut self, block: &BlockId, offset: i32) -> Result<String> {
        self.cm.s_lock(block)?;
        let buffer = self.my_buffers.buffer(block);
        Ok(buffer.contents.get_string(offset))
    }
    pub fn set_string(
        &mut self,
        block: &BlockId,
        offset: i32,
        value: &str,
        log: bool,
    ) -> Result<()> {
        self.cm.x_lock(block)?;
        let buffer = self.my_buffers.buffer_mut(block);
        let mut lsn = -1;
        if log {
            lsn = self.rm.set_string(buffer, offset, value)?;
        }
        buffer.contents.set_string(offset, value);
        buffer.set_modified(self.tx_num, lsn);
        Ok(())
    }

    pub fn get_bool(&mut self, block: &BlockId, offset: i32) -> Result<bool> {
        self.cm.s_lock(block)?;
        let buffer = self.my_buffers.buffer(block);
        Ok(buffer.contents.get_bool(offset))
    }
    pub fn set_bool(&mut self, block: &BlockId, offset: i32, value: bool, log: bool) -> Result<()> {
        self.cm.x_lock(block)?;
        let buffer = self.my_buffers.buffer_mut(block);
        let mut lsn = -1;
        if log {
            lsn = self.rm.set_bool(buffer, offset, value)?;
        }
        buffer.contents.set_bool(offset, value);
        buffer.set_modified(self.tx_num, lsn);
        Ok(())
    }

    pub fn get_double(&mut self, block: &BlockId, offset: i32) -> Result<f64> {
        self.cm.s_lock(block)?;
        let buffer = self.my_buffers.buffer(block);
        Ok(buffer.contents.get_double(offset))
    }
    pub fn set_double(
        &mut self,
        block: &BlockId,
        offset: i32,
        value: f64,
        log: bool,
    ) -> Result<()> {
        self.cm.x_lock(block)?;
        let buffer = self.my_buffers.buffer_mut(block);
        let mut lsn = -1;
        if log {
            lsn = self.rm.set_double(buffer, offset, value)?;
        }
        buffer.contents.set_double(offset, value);
        buffer.set_modified(self.tx_num, lsn);
        Ok(())
    }

    pub fn get_date(&mut self, block: &BlockId, offset: i32) -> Result<Option<chrono::NaiveDate>> {
        self.cm.s_lock(block)?;
        let buffer = self.my_buffers.buffer(block);
        Ok(buffer.contents.get_date(offset))
    }
    pub fn set_date(
        &mut self,
        block: &BlockId,
        offset: i32,
        value: &Option<chrono::NaiveDate>,
        log: bool,
    ) -> Result<()> {
        self.cm.x_lock(block)?;
        let buffer = self.my_buffers.buffer_mut(block);
        let mut lsn = -1;
        if log {
            lsn = self.rm.set_date(buffer, offset, value)?;
        }
        buffer.contents.set_date(offset, value);
        buffer.set_modified(self.tx_num, lsn);
        Ok(())
    }

    pub fn get_time(&mut self, block: &BlockId, offset: i32) -> Result<Option<chrono::NaiveTime>> {
        self.cm.s_lock(block)?;
        let buffer = self.my_buffers.buffer(block);
        Ok(buffer.contents.get_time(offset))
    }
    pub fn set_time(
        &mut self,
        block: &BlockId,
        offset: i32,
        value: &Option<chrono::NaiveTime>,
        log: bool,
    ) -> Result<()> {
        self.cm.x_lock(block)?;
        let buffer = self.my_buffers.buffer_mut(block);
        let mut lsn = -1;
        if log {
            lsn = self.rm.set_time(buffer, offset, value)?;
        }
        buffer.contents.set_time(offset, value);
        buffer.set_modified(self.tx_num, lsn);
        Ok(())
    }

    pub fn get_datetime(
        &mut self,
        block: &BlockId,
        offset: i32,
    ) -> Result<Option<chrono::DateTime<chrono::FixedOffset>>> {
        self.cm.s_lock(block)?;
        let buffer = self.my_buffers.buffer(block);
        Ok(buffer.contents.get_datetime(offset))
    }
    pub fn set_datetime(
        &mut self,
        block: &BlockId,
        offset: i32,
        value: &Option<chrono::DateTime<chrono::FixedOffset>>,
        log: bool,
    ) -> Result<()> {
        self.cm.x_lock(block)?;
        let buffer = self.my_buffers.buffer_mut(block);
        let mut lsn = -1;
        if log {
            lsn = self.rm.set_datetime(buffer, offset, value)?;
        }
        buffer.contents.set_datetime(offset, value);
        buffer.set_modified(self.tx_num, lsn);
        Ok(())
    }

    pub fn get_json(&mut self, block: &BlockId, offset: i32) -> Result<Option<serde_json::Value>> {
        self.cm.s_lock(block)?;
        let buffer = self.my_buffers.buffer(block);
        Ok(buffer.contents.get_json(offset))
    }
    pub fn set_json(
        &mut self,
        block: &BlockId,
        offset: i32,
        value: &Option<serde_json::Value>,
        log: bool,
    ) -> Result<()> {
        self.cm.x_lock(block)?;
        let buffer = self.my_buffers.buffer_mut(block);
        let mut lsn = -1;
        if log {
            lsn = self.rm.set_json(buffer, offset, value)?;
        }
        buffer.contents.set_json(offset, value);
        buffer.set_modified(self.tx_num, lsn);
        Ok(())
    }

    pub fn size(&mut self, filename: &str) -> Result<i32> {
        let dummy = BlockId::new(filename.to_string(), END_OF_FILE);
        self.cm.x_lock(&dummy)?;
        let fm = Arc::as_ptr(&self.fm) as *mut FileManager;
        unsafe { (*fm).length(filename) }
    }

    pub fn append(&mut self, filename: &str) -> Result<BlockId> {
        let dummy = BlockId::new(filename.to_string(), END_OF_FILE);
        self.cm.x_lock(&dummy)?;
        let fm = Arc::as_ptr(&self.fm) as *mut FileManager;
        unsafe { (*fm).append(filename) }
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

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    mod int {
        use super::*;

        #[test]
        fn set_and_get() {
            let fm = Arc::new(FileManager::new(
                PathBuf::from("testdata/tx/transaction/int/set_and_get"),
                400,
            ));
            let lm = Arc::new(LogManager::new(fm.clone(), "templog".to_string()));
            let bm = Arc::new(BufferManager::new(fm.clone(), lm.clone(), 8));
            let lock_table = Arc::new(LockTable::new());
            let block = BlockId::new("tempfile".to_string(), 0);

            let mut tx = Transaction::new(fm.clone(), lm.clone(), bm.clone(), lock_table.clone());
            tx.pin(&block).unwrap();
            tx.set_int(&block, 0, 123, true).unwrap();

            assert_eq!(tx.get_int(&block, 0).unwrap(), 123);
        }

        #[test]
        fn rollback() {
            let fm = Arc::new(FileManager::new(
                PathBuf::from("testdata/tx/transaction/int/rollback"),
                400,
            ));
            let lm = Arc::new(LogManager::new(fm.clone(), "templog".to_string()));
            let bm = Arc::new(BufferManager::new(fm.clone(), lm.clone(), 8));
            let lock_table = Arc::new(LockTable::new());
            let block = BlockId::new("tempfile".to_string(), 0);

            let mut tx = Transaction::new(fm.clone(), lm.clone(), bm.clone(), lock_table.clone());
            tx.pin(&block).unwrap();
            tx.set_int(&block, 0, 123, true).unwrap();
            tx.commit().unwrap();

            let mut tx = Transaction::new(fm.clone(), lm.clone(), bm.clone(), lock_table.clone());
            tx.pin(&block).unwrap();
            tx.set_int(&block, 0, 456, true).unwrap();
            tx.rollback();

            let mut tx = Transaction::new(fm.clone(), lm.clone(), bm.clone(), lock_table.clone());
            tx.pin(&block).unwrap();
            assert_eq!(tx.get_int(&block, 0).unwrap(), 123);
        }

        #[test]
        fn recover() {
            let fm = Arc::new(FileManager::new(
                PathBuf::from("testdata/tx/transaction/int/recover"),
                400,
            ));
            let lm = Arc::new(LogManager::new(fm.clone(), "templog".to_string()));
            let bm = Arc::new(BufferManager::new(fm.clone(), lm.clone(), 8));
            let lock_table = Arc::new(LockTable::new());
            let block = BlockId::new("tempfile".to_string(), 0);

            let mut tx = Transaction::new(fm.clone(), lm.clone(), bm.clone(), lock_table.clone());
            tx.pin(&block).unwrap();
            tx.set_int(&block, 0, 123, true).unwrap();
            tx.commit().unwrap();

            let mut tx = Transaction::new(fm.clone(), lm.clone(), bm.clone(), lock_table.clone());
            tx.pin(&block).unwrap();
            tx.set_int(&block, 0, 456, true).unwrap();
            tx.unpin(&block);
            tx.cm.release();

            let tx = Transaction::new(fm.clone(), lm.clone(), bm.clone(), lock_table.clone());
            tx.recover();

            let mut tx = Transaction::new(fm.clone(), lm.clone(), bm.clone(), lock_table.clone());
            tx.pin(&block).unwrap();
            assert_eq!(tx.get_int(&block, 0).unwrap(), 123);
        }
    }

    mod bytes {
        use super::*;

        #[test]
        fn set_and_get() {
            let fm = Arc::new(FileManager::new(
                PathBuf::from("testdata/tx/transaction/bytes/set_and_get"),
                400,
            ));
            let lm = Arc::new(LogManager::new(fm.clone(), "templog".to_string()));
            let bm = Arc::new(BufferManager::new(fm.clone(), lm.clone(), 8));
            let lock_table = Arc::new(LockTable::new());
            let block = BlockId::new("tempfile".to_string(), 0);

            let mut tx = Transaction::new(fm.clone(), lm.clone(), bm.clone(), lock_table.clone());
            tx.pin(&block).unwrap();
            tx.set_bytes(&block, 0, &[1, 2, 3], true).unwrap();

            assert_eq!(tx.get_bytes(&block, 0).unwrap(), &[1, 2, 3]);
        }

        #[test]
        fn rollback() {
            let fm = Arc::new(FileManager::new(
                PathBuf::from("testdata/tx/transaction/bytes/rollback"),
                400,
            ));
            let lm = Arc::new(LogManager::new(fm.clone(), "templog".to_string()));
            let bm = Arc::new(BufferManager::new(fm.clone(), lm.clone(), 8));
            let lock_table = Arc::new(LockTable::new());
            let block = BlockId::new("tempfile".to_string(), 0);

            let mut tx = Transaction::new(fm.clone(), lm.clone(), bm.clone(), lock_table.clone());
            tx.pin(&block).unwrap();
            tx.set_bytes(&block, 0, &[1, 2, 3], true).unwrap();
            tx.commit().unwrap();

            let mut tx = Transaction::new(fm.clone(), lm.clone(), bm.clone(), lock_table.clone());
            tx.pin(&block).unwrap();
            tx.set_bytes(&block, 0, &[4, 5, 6], true).unwrap();
            tx.rollback();

            let mut tx = Transaction::new(fm.clone(), lm.clone(), bm.clone(), lock_table.clone());
            tx.pin(&block).unwrap();
            assert_eq!(tx.get_bytes(&block, 0).unwrap(), &[1, 2, 3]);
        }

        #[test]
        fn recover() {
            let fm = Arc::new(FileManager::new(
                PathBuf::from("testdata/tx/transaction/bytes/recover"),
                400,
            ));
            let lm = Arc::new(LogManager::new(fm.clone(), "templog".to_string()));
            let bm = Arc::new(BufferManager::new(fm.clone(), lm.clone(), 8));
            let lock_table = Arc::new(LockTable::new());
            let block = BlockId::new("tempfile".to_string(), 0);

            let mut tx = Transaction::new(fm.clone(), lm.clone(), bm.clone(), lock_table.clone());
            tx.pin(&block).unwrap();
            tx.set_bytes(&block, 0, &[1, 2, 3], true).unwrap();
            tx.commit().unwrap();

            let mut tx = Transaction::new(fm.clone(), lm.clone(), bm.clone(), lock_table.clone());
            tx.pin(&block).unwrap();
            tx.set_bytes(&block, 0, &[4, 5, 6], true).unwrap();
            tx.unpin(&block);
            tx.cm.release();

            let tx = Transaction::new(fm.clone(), lm.clone(), bm.clone(), lock_table.clone());
            tx.recover();

            let mut tx = Transaction::new(fm.clone(), lm.clone(), bm.clone(), lock_table.clone());
            tx.pin(&block).unwrap();
            assert_eq!(tx.get_bytes(&block, 0).unwrap(), &[1, 2, 3]);
        }
    }

    mod string {
        use super::*;

        #[test]
        fn set_and_get() {
            let fm = Arc::new(FileManager::new(
                PathBuf::from("testdata/tx/transaction/string/set_and_get"),
                400,
            ));
            let lm = Arc::new(LogManager::new(fm.clone(), "templog".to_string()));
            let bm = Arc::new(BufferManager::new(fm.clone(), lm.clone(), 8));
            let lock_table = Arc::new(LockTable::new());
            let block = BlockId::new("tempfile".to_string(), 0);

            let mut tx = Transaction::new(fm.clone(), lm.clone(), bm.clone(), lock_table.clone());
            tx.pin(&block).unwrap();
            tx.set_string(&block, 0, "abc", true).unwrap();

            assert_eq!(tx.get_string(&block, 0).unwrap(), "abc");
        }

        #[test]
        fn rollback() {
            let fm = Arc::new(FileManager::new(
                PathBuf::from("testdata/tx/transaction/string/rollback"),
                400,
            ));
            let lm = Arc::new(LogManager::new(fm.clone(), "templog".to_string()));
            let bm = Arc::new(BufferManager::new(fm.clone(), lm.clone(), 8));
            let lock_table = Arc::new(LockTable::new());
            let block = BlockId::new("tempfile".to_string(), 0);

            let mut tx = Transaction::new(fm.clone(), lm.clone(), bm.clone(), lock_table.clone());
            tx.pin(&block).unwrap();
            tx.set_string(&block, 0, "abc", true).unwrap();
            tx.commit().unwrap();

            let mut tx = Transaction::new(fm.clone(), lm.clone(), bm.clone(), lock_table.clone());
            tx.pin(&block).unwrap();
            tx.set_string(&block, 0, "def", true).unwrap();
            tx.rollback();

            let mut tx = Transaction::new(fm.clone(), lm.clone(), bm.clone(), lock_table.clone());
            tx.pin(&block).unwrap();
            assert_eq!(tx.get_string(&block, 0).unwrap(), "abc");
        }

        #[test]
        fn recover() {
            let fm = Arc::new(FileManager::new(
                PathBuf::from("testdata/tx/transaction/string/recover"),
                400,
            ));
            let lm = Arc::new(LogManager::new(fm.clone(), "templog".to_string()));
            let bm = Arc::new(BufferManager::new(fm.clone(), lm.clone(), 8));
            let lock_table = Arc::new(LockTable::new());
            let block = BlockId::new("tempfile".to_string(), 0);

            let mut tx = Transaction::new(fm.clone(), lm.clone(), bm.clone(), lock_table.clone());
            tx.pin(&block).unwrap();
            tx.set_string(&block, 0, "abc", true).unwrap();
            tx.commit().unwrap();

            let mut tx = Transaction::new(fm.clone(), lm.clone(), bm.clone(), lock_table.clone());
            tx.pin(&block).unwrap();
            tx.set_string(&block, 0, "def", true).unwrap();
            tx.unpin(&block);
            tx.cm.release();

            let tx = Transaction::new(fm.clone(), lm.clone(), bm.clone(), lock_table.clone());
            tx.recover();

            let mut tx = Transaction::new(fm.clone(), lm.clone(), bm.clone(), lock_table.clone());
            tx.pin(&block).unwrap();
            assert_eq!(tx.get_string(&block, 0).unwrap(), "abc");
        }
    }

    mod bool {
        use super::*;

        #[test]
        fn set_and_get() {
            let fm = Arc::new(FileManager::new(
                PathBuf::from("testdata/tx/transaction/bool/set_and_get"),
                400,
            ));
            let lm = Arc::new(LogManager::new(fm.clone(), "templog".to_string()));
            let bm = Arc::new(BufferManager::new(fm.clone(), lm.clone(), 8));
            let lock_table = Arc::new(LockTable::new());
            let block = BlockId::new("tempfile".to_string(), 0);

            let mut tx = Transaction::new(fm.clone(), lm.clone(), bm.clone(), lock_table.clone());
            tx.pin(&block).unwrap();
            tx.set_bool(&block, 0, true, true).unwrap();

            assert!(tx.get_bool(&block, 0).unwrap());
        }

        #[test]
        fn rollback() {
            let fm = Arc::new(FileManager::new(
                PathBuf::from("testdata/tx/transaction/bool/rollback"),
                400,
            ));
            let lm = Arc::new(LogManager::new(fm.clone(), "templog".to_string()));
            let bm = Arc::new(BufferManager::new(fm.clone(), lm.clone(), 8));
            let lock_table = Arc::new(LockTable::new());
            let block = BlockId::new("tempfile".to_string(), 0);

            let mut tx = Transaction::new(fm.clone(), lm.clone(), bm.clone(), lock_table.clone());
            tx.pin(&block).unwrap();
            tx.set_bool(&block, 0, true, true).unwrap();
            tx.commit().unwrap();

            let mut tx = Transaction::new(fm.clone(), lm.clone(), bm.clone(), lock_table.clone());
            tx.pin(&block).unwrap();
            tx.set_bool(&block, 0, false, true).unwrap();
            tx.rollback();

            let mut tx = Transaction::new(fm.clone(), lm.clone(), bm.clone(), lock_table.clone());
            tx.pin(&block).unwrap();
            assert!(tx.get_bool(&block, 0).unwrap());
        }

        #[test]
        fn recover() {
            let fm = Arc::new(FileManager::new(
                PathBuf::from("testdata/tx/transaction/bool/recover"),
                400,
            ));
            let lm = Arc::new(LogManager::new(fm.clone(), "templog".to_string()));
            let bm = Arc::new(BufferManager::new(fm.clone(), lm.clone(), 8));
            let lock_table = Arc::new(LockTable::new());
            let block = BlockId::new("tempfile".to_string(), 0);

            let mut tx = Transaction::new(fm.clone(), lm.clone(), bm.clone(), lock_table.clone());
            tx.pin(&block).unwrap();
            tx.set_bool(&block, 0, true, true).unwrap();
            tx.commit().unwrap();

            let mut tx = Transaction::new(fm.clone(), lm.clone(), bm.clone(), lock_table.clone());
            tx.pin(&block).unwrap();
            tx.set_bool(&block, 0, false, true).unwrap();
            tx.unpin(&block);
            tx.cm.release();

            let tx = Transaction::new(fm.clone(), lm.clone(), bm.clone(), lock_table.clone());
            tx.recover();

            let mut tx = Transaction::new(fm.clone(), lm.clone(), bm.clone(), lock_table.clone());
            tx.pin(&block).unwrap();
            assert!(tx.get_bool(&block, 0).unwrap());
        }
    }

    mod double {
        use super::*;

        #[test]
        fn set_and_get() {
            let fm = Arc::new(FileManager::new(
                PathBuf::from("testdata/tx/transaction/double/set_and_get"),
                400,
            ));
            let lm = Arc::new(LogManager::new(fm.clone(), "templog".to_string()));
            let bm = Arc::new(BufferManager::new(fm.clone(), lm.clone(), 8));
            let lock_table = Arc::new(LockTable::new());
            let block = BlockId::new("tempfile".to_string(), 0);

            let mut tx = Transaction::new(fm.clone(), lm.clone(), bm.clone(), lock_table.clone());
            tx.pin(&block).unwrap();
            tx.set_double(&block, 0, 1.23, true).unwrap();

            assert_eq!(tx.get_double(&block, 0).unwrap(), 1.23);
        }

        #[test]
        fn rollback() {
            let fm = Arc::new(FileManager::new(
                PathBuf::from("testdata/tx/transaction/double/rollback"),
                400,
            ));
            let lm = Arc::new(LogManager::new(fm.clone(), "templog".to_string()));
            let bm = Arc::new(BufferManager::new(fm.clone(), lm.clone(), 8));
            let lock_table = Arc::new(LockTable::new());
            let block = BlockId::new("tempfile".to_string(), 0);

            let mut tx = Transaction::new(fm.clone(), lm.clone(), bm.clone(), lock_table.clone());
            tx.pin(&block).unwrap();
            tx.set_double(&block, 0, 1.23, true).unwrap();
            tx.commit().unwrap();

            let mut tx = Transaction::new(fm.clone(), lm.clone(), bm.clone(), lock_table.clone());
            tx.pin(&block).unwrap();
            tx.set_double(&block, 0, 4.56, true).unwrap();
            tx.rollback();

            let mut tx = Transaction::new(fm.clone(), lm.clone(), bm.clone(), lock_table.clone());
            tx.pin(&block).unwrap();
            assert_eq!(tx.get_double(&block, 0).unwrap(), 1.23);
        }

        #[test]
        fn recover() {
            let fm = Arc::new(FileManager::new(
                PathBuf::from("testdata/tx/transaction/double/recover"),
                400,
            ));
            let lm = Arc::new(LogManager::new(fm.clone(), "templog".to_string()));
            let bm = Arc::new(BufferManager::new(fm.clone(), lm.clone(), 8));
            let lock_table = Arc::new(LockTable::new());
            let block = BlockId::new("tempfile".to_string(), 0);

            let mut tx = Transaction::new(fm.clone(), lm.clone(), bm.clone(), lock_table.clone());
            tx.pin(&block).unwrap();
            tx.set_double(&block, 0, 1.23, true).unwrap();
            tx.commit().unwrap();

            let mut tx = Transaction::new(fm.clone(), lm.clone(), bm.clone(), lock_table.clone());
            tx.pin(&block).unwrap();
            tx.set_double(&block, 0, 4.56, true).unwrap();
            tx.unpin(&block);
            tx.cm.release();

            let tx = Transaction::new(fm.clone(), lm.clone(), bm.clone(), lock_table.clone());
            tx.recover();

            let mut tx = Transaction::new(fm.clone(), lm.clone(), bm.clone(), lock_table.clone());
            tx.pin(&block).unwrap();
            assert_eq!(tx.get_double(&block, 0).unwrap(), 1.23);
        }
    }

    mod date {
        use super::*;

        #[test]
        fn set_and_get() {
            let fm = Arc::new(FileManager::new(
                PathBuf::from("testdata/tx/transaction/date/set_and_get"),
                400,
            ));
            let lm = Arc::new(LogManager::new(fm.clone(), "templog".to_string()));
            let bm = Arc::new(BufferManager::new(fm.clone(), lm.clone(), 8));
            let lock_table = Arc::new(LockTable::new());
            let block = BlockId::new("tempfile".to_string(), 0);

            let mut tx = Transaction::new(fm.clone(), lm.clone(), bm.clone(), lock_table.clone());
            tx.pin(&block).unwrap();
            tx.set_date(
                &block,
                0,
                &chrono::NaiveDate::from_ymd_opt(2021, 1, 1),
                true,
            )
            .unwrap();

            assert_eq!(
                tx.get_date(&block, 0).unwrap(),
                chrono::NaiveDate::from_ymd_opt(2021, 1, 1)
            );
        }

        #[test]
        fn rollback() {
            let fm = Arc::new(FileManager::new(
                PathBuf::from("testdata/tx/transaction/date/rollback"),
                400,
            ));
            let lm = Arc::new(LogManager::new(fm.clone(), "templog".to_string()));
            let bm = Arc::new(BufferManager::new(fm.clone(), lm.clone(), 8));
            let lock_table = Arc::new(LockTable::new());
            let block = BlockId::new("tempfile".to_string(), 0);
            let date1 = chrono::NaiveDate::from_ymd_opt(2021, 1, 1);
            let date2 = chrono::NaiveDate::from_ymd_opt(2021, 12, 31);

            let mut tx = Transaction::new(fm.clone(), lm.clone(), bm.clone(), lock_table.clone());
            tx.pin(&block).unwrap();
            tx.set_date(&block, 0, &date1, true).unwrap();
            tx.commit().unwrap();

            let mut tx = Transaction::new(fm.clone(), lm.clone(), bm.clone(), lock_table.clone());
            tx.pin(&block).unwrap();
            tx.set_date(&block, 0, &date2, true).unwrap();
            tx.rollback();

            let mut tx = Transaction::new(fm.clone(), lm.clone(), bm.clone(), lock_table.clone());
            tx.pin(&block).unwrap();
            assert_eq!(tx.get_date(&block, 0).unwrap(), date1);
        }

        #[test]
        fn recover() {
            let fm = Arc::new(FileManager::new(
                PathBuf::from("testdata/tx/transaction/date/recover"),
                400,
            ));
            let lm = Arc::new(LogManager::new(fm.clone(), "templog".to_string()));
            let bm = Arc::new(BufferManager::new(fm.clone(), lm.clone(), 8));
            let lock_table = Arc::new(LockTable::new());
            let block = BlockId::new("tempfile".to_string(), 0);
            let date1 = chrono::NaiveDate::from_ymd_opt(2021, 1, 1);
            let date2 = chrono::NaiveDate::from_ymd_opt(2021, 12, 31);

            let mut tx = Transaction::new(fm.clone(), lm.clone(), bm.clone(), lock_table.clone());
            tx.pin(&block).unwrap();
            tx.set_date(&block, 0, &date1, true).unwrap();
            tx.commit().unwrap();

            let mut tx = Transaction::new(fm.clone(), lm.clone(), bm.clone(), lock_table.clone());
            tx.pin(&block).unwrap();
            tx.set_date(&block, 0, &date2, true).unwrap();
            tx.unpin(&block);
            tx.cm.release();

            let tx = Transaction::new(fm.clone(), lm.clone(), bm.clone(), lock_table.clone());
            tx.recover();

            let mut tx = Transaction::new(fm.clone(), lm.clone(), bm.clone(), lock_table.clone());
            tx.pin(&block).unwrap();
            assert_eq!(tx.get_date(&block, 0).unwrap(), date1);
        }
    }

    mod time {
        use super::*;

        #[test]
        fn set_and_get() {
            let fm = Arc::new(FileManager::new(
                PathBuf::from("testdata/tx/transaction/time/set_and_get"),
                400,
            ));
            let lm = Arc::new(LogManager::new(fm.clone(), "templog".to_string()));
            let bm = Arc::new(BufferManager::new(fm.clone(), lm.clone(), 8));
            let lock_table = Arc::new(LockTable::new());
            let block = BlockId::new("tempfile".to_string(), 0);

            let mut tx = Transaction::new(fm.clone(), lm.clone(), bm.clone(), lock_table.clone());
            tx.pin(&block).unwrap();
            tx.set_time(
                &block,
                0,
                &chrono::NaiveTime::from_hms_opt(12, 34, 56),
                true,
            )
            .unwrap();

            assert_eq!(
                tx.get_time(&block, 0).unwrap(),
                chrono::NaiveTime::from_hms_opt(12, 34, 56)
            );
        }

        #[test]
        fn rollback() {
            let fm = Arc::new(FileManager::new(
                PathBuf::from("testdata/tx/transaction/time/rollback"),
                400,
            ));
            let lm = Arc::new(LogManager::new(fm.clone(), "templog".to_string()));
            let bm = Arc::new(BufferManager::new(fm.clone(), lm.clone(), 8));
            let lock_table = Arc::new(LockTable::new());
            let block = BlockId::new("tempfile".to_string(), 0);
            let time1 = chrono::NaiveTime::from_hms_opt(1, 2, 3);
            let time2 = chrono::NaiveTime::from_hms_opt(4, 5, 6);

            let mut tx = Transaction::new(fm.clone(), lm.clone(), bm.clone(), lock_table.clone());
            tx.pin(&block).unwrap();
            tx.set_time(&block, 0, &time1, true).unwrap();
            tx.commit().unwrap();

            let mut tx = Transaction::new(fm.clone(), lm.clone(), bm.clone(), lock_table.clone());
            tx.pin(&block).unwrap();
            tx.set_time(&block, 0, &time2, true).unwrap();
            tx.rollback();

            let mut tx = Transaction::new(fm.clone(), lm.clone(), bm.clone(), lock_table.clone());
            tx.pin(&block).unwrap();
            assert_eq!(tx.get_time(&block, 0).unwrap(), time1);
        }

        #[test]
        fn recover() {
            let fm = Arc::new(FileManager::new(
                PathBuf::from("testdata/tx/transaction/time/recover"),
                400,
            ));
            let lm = Arc::new(LogManager::new(fm.clone(), "templog".to_string()));
            let bm = Arc::new(BufferManager::new(fm.clone(), lm.clone(), 8));
            let lock_table = Arc::new(LockTable::new());
            let block = BlockId::new("tempfile".to_string(), 0);
            let time1 = chrono::NaiveTime::from_hms_opt(1, 2, 3);
            let time2 = chrono::NaiveTime::from_hms_opt(4, 5, 6);

            let mut tx = Transaction::new(fm.clone(), lm.clone(), bm.clone(), lock_table.clone());
            tx.pin(&block).unwrap();
            tx.set_time(&block, 0, &time1, true).unwrap();
            tx.commit().unwrap();

            let mut tx = Transaction::new(fm.clone(), lm.clone(), bm.clone(), lock_table.clone());
            tx.pin(&block).unwrap();
            tx.set_time(&block, 0, &time2, true).unwrap();
            tx.unpin(&block);
            tx.cm.release();

            let tx = Transaction::new(fm.clone(), lm.clone(), bm.clone(), lock_table.clone());
            tx.recover();

            let mut tx = Transaction::new(fm.clone(), lm.clone(), bm.clone(), lock_table.clone());
            tx.pin(&block).unwrap();
            assert_eq!(tx.get_time(&block, 0).unwrap(), time1);
        }
    }

    mod datetime {
        use super::*;

        #[test]
        fn set_and_get() {
            let fm = Arc::new(FileManager::new(
                PathBuf::from("testdata/tx/transaction/datetime/set_and_get"),
                400,
            ));
            let lm = Arc::new(LogManager::new(fm.clone(), "templog".to_string()));
            let bm = Arc::new(BufferManager::new(fm.clone(), lm.clone(), 8));
            let lock_table = Arc::new(LockTable::new());
            let block = BlockId::new("tempfile".to_string(), 0);
            let now = chrono::Utc::now().fixed_offset();

            let mut tx = Transaction::new(fm.clone(), lm.clone(), bm.clone(), lock_table.clone());
            tx.pin(&block).unwrap();
            tx.set_datetime(&block, 0, &Some(now), true).unwrap();

            assert_eq!(tx.get_datetime(&block, 0).unwrap().unwrap(), now);
        }

        #[test]
        fn rollback() {
            let fm = Arc::new(FileManager::new(
                PathBuf::from("testdata/tx/transaction/datetime/rollback"),
                400,
            ));
            let lm = Arc::new(LogManager::new(fm.clone(), "templog".to_string()));
            let bm = Arc::new(BufferManager::new(fm.clone(), lm.clone(), 8));
            let lock_table = Arc::new(LockTable::new());
            let block = BlockId::new("tempfile".to_string(), 0);
            let datetime1 = chrono::DateTime::parse_from_rfc3339("2021-01-01T00:00:00Z").unwrap();
            let datetime2 = chrono::DateTime::parse_from_rfc3339("2021-12-31T23:59:59Z").unwrap();

            let mut tx = Transaction::new(fm.clone(), lm.clone(), bm.clone(), lock_table.clone());
            tx.pin(&block).unwrap();
            tx.set_datetime(&block, 0, &Some(datetime1), true).unwrap();
            tx.commit().unwrap();

            let mut tx = Transaction::new(fm.clone(), lm.clone(), bm.clone(), lock_table.clone());
            tx.pin(&block).unwrap();
            tx.set_datetime(&block, 0, &Some(datetime2), true).unwrap();
            tx.rollback();

            let mut tx = Transaction::new(fm.clone(), lm.clone(), bm.clone(), lock_table.clone());
            tx.pin(&block).unwrap();
            assert_eq!(tx.get_datetime(&block, 0).unwrap(), Some(datetime1));
        }

        #[test]
        fn recover() {
            let fm = Arc::new(FileManager::new(
                PathBuf::from("testdata/tx/transaction/datetime/recover"),
                400,
            ));
            let lm = Arc::new(LogManager::new(fm.clone(), "templog".to_string()));
            let bm = Arc::new(BufferManager::new(fm.clone(), lm.clone(), 8));
            let lock_table = Arc::new(LockTable::new());
            let block = BlockId::new("tempfile".to_string(), 0);
            let datetime1 = chrono::DateTime::parse_from_rfc3339("2021-01-01T00:00:00Z").unwrap();
            let datetime2 = chrono::DateTime::parse_from_rfc3339("2021-12-31T23:59:59Z").unwrap();

            let mut tx = Transaction::new(fm.clone(), lm.clone(), bm.clone(), lock_table.clone());
            tx.pin(&block).unwrap();
            tx.set_datetime(&block, 0, &Some(datetime1), true).unwrap();
            tx.commit().unwrap();

            let mut tx = Transaction::new(fm.clone(), lm.clone(), bm.clone(), lock_table.clone());
            tx.pin(&block).unwrap();
            tx.set_datetime(&block, 0, &Some(datetime2), true).unwrap();
            tx.unpin(&block);
            tx.cm.release();

            let tx = Transaction::new(fm.clone(), lm.clone(), bm.clone(), lock_table.clone());
            tx.recover();

            let mut tx = Transaction::new(fm.clone(), lm.clone(), bm.clone(), lock_table.clone());
            tx.pin(&block).unwrap();
            assert_eq!(tx.get_datetime(&block, 0).unwrap(), Some(datetime1));
        }
    }

    mod json {
        use super::*;

        #[test]
        fn set_and_get() {
            let fm = Arc::new(FileManager::new(
                PathBuf::from("testdata/tx/transaction/json/set_and_get"),
                400,
            ));
            let lm = Arc::new(LogManager::new(fm.clone(), "templog".to_string()));
            let bm = Arc::new(BufferManager::new(fm.clone(), lm.clone(), 8));
            let lock_table = Arc::new(LockTable::new());
            let block = BlockId::new("tempfile".to_string(), 0);

            let mut tx = Transaction::new(fm.clone(), lm.clone(), bm.clone(), lock_table.clone());
            tx.pin(&block).unwrap();
            tx.set_json(&block, 0, &Some(serde_json::json!({"key": "value"})), true)
                .unwrap();

            assert_eq!(
                tx.get_json(&block, 0).unwrap().unwrap(),
                serde_json::json!({"key": "value"})
            );
        }

        #[test]
        fn rollback() {
            let fm = Arc::new(FileManager::new(
                PathBuf::from("testdata/tx/transaction/json/rollback"),
                400,
            ));
            let lm = Arc::new(LogManager::new(fm.clone(), "templog".to_string()));
            let bm = Arc::new(BufferManager::new(fm.clone(), lm.clone(), 8));
            let lock_table = Arc::new(LockTable::new());
            let block = BlockId::new("tempfile".to_string(), 0);

            let mut tx = Transaction::new(fm.clone(), lm.clone(), bm.clone(), lock_table.clone());
            tx.pin(&block).unwrap();
            tx.set_json(&block, 0, &Some(serde_json::json!({"key": "value"})), true)
                .unwrap();
            tx.commit().unwrap();

            let mut tx = Transaction::new(fm.clone(), lm.clone(), bm.clone(), lock_table.clone());
            tx.pin(&block).unwrap();
            tx.set_json(&block, 0, &Some(serde_json::json!({"key": "value2"})), true)
                .unwrap();
            tx.rollback();

            let mut tx = Transaction::new(fm.clone(), lm.clone(), bm.clone(), lock_table.clone());
            tx.pin(&block).unwrap();
            assert_eq!(
                tx.get_json(&block, 0).unwrap(),
                Some(serde_json::json!({"key": "value"}))
            );
        }

        #[test]
        fn recover() {
            let fm = Arc::new(FileManager::new(
                PathBuf::from("testdata/tx/transaction/json/recover"),
                400,
            ));
            let lm = Arc::new(LogManager::new(fm.clone(), "templog".to_string()));
            let bm = Arc::new(BufferManager::new(fm.clone(), lm.clone(), 8));
            let lock_table = Arc::new(LockTable::new());
            let block = BlockId::new("tempfile".to_string(), 0);

            let mut tx = Transaction::new(fm.clone(), lm.clone(), bm.clone(), lock_table.clone());
            tx.pin(&block).unwrap();
            tx.set_json(&block, 0, &Some(serde_json::json!({"key": "value"})), true)
                .unwrap();
            tx.commit().unwrap();

            let mut tx = Transaction::new(fm.clone(), lm.clone(), bm.clone(), lock_table.clone());
            tx.pin(&block).unwrap();
            tx.set_json(&block, 0, &Some(serde_json::json!({"key": "value2"})), true)
                .unwrap();
            tx.unpin(&block);
            tx.cm.release();

            let tx = Transaction::new(fm.clone(), lm.clone(), bm.clone(), lock_table.clone());
            tx.recover();

            let mut tx = Transaction::new(fm.clone(), lm.clone(), bm.clone(), lock_table.clone());
            tx.pin(&block).unwrap();
            assert_eq!(
                tx.get_json(&block, 0).unwrap().unwrap(),
                serde_json::json!({"key": "value"})
            );
        }
    }

    mod checkpoint {

        use super::*;

        #[test]
        fn error_if_blocked() {
            let fm = Arc::new(FileManager::new(
                PathBuf::from("testdata/tx/transaction/checkpoint/error_if_blocked"),
                400,
            ));
            let lm = Arc::new(LogManager::new(fm.clone(), "templog".to_string()));
            let bm = Arc::new(BufferManager::new(fm.clone(), lm.clone(), 8));
            let lock_table = Arc::new(LockTable::new());

            let tx = Transaction::new(fm.clone(), lm.clone(), bm.clone(), lock_table.clone());
            let res = Transaction::checkpoint(bm.clone(), lm.clone());

            assert!(res.is_err());
        }

        #[test]
        fn ok_if_not_blocked() {
            let fm = Arc::new(FileManager::new(
                PathBuf::from("testdata/tx/transaction/checkpoint/ok_if_not_blocked"),
                400,
            ));
            let lm = Arc::new(LogManager::new(fm.clone(), "templog".to_string()));
            let bm = Arc::new(BufferManager::new(fm.clone(), lm.clone(), 8));
            let lock_table = Arc::new(LockTable::new());

            let tx = Transaction::new(fm.clone(), lm.clone(), bm.clone(), lock_table.clone());
            let t = thread::spawn(move || {
                let res = Transaction::checkpoint(bm.clone(), lm.clone());
                assert!(res.is_ok());
            });
            drop(tx);
            t.join().unwrap();
        }

        #[test]
        fn new_tx_is_kept_waiting() {
            let fm = Arc::new(FileManager::new(
                PathBuf::from("testdata/tx/transaction/checkpoint/new_tx_is_kept_waiting"),
                400,
            ));
            let lm = Arc::new(LogManager::new(fm.clone(), "templog".to_string()));
            let bm = Arc::new(BufferManager::new(fm.clone(), lm.clone(), 8));
            let lock_table = Arc::new(LockTable::new());

            let tx1 = Transaction::new(fm.clone(), lm.clone(), bm.clone(), lock_table.clone());
            let t1 = {
                let bm = bm.clone();
                let lm = lm.clone();
                thread::spawn(move || {
                    let res = Transaction::checkpoint(bm, lm);
                    assert!(res.is_ok());
                })
            };
            let t2 = thread::spawn(move || {
                let tx2 = Transaction::new(fm.clone(), lm.clone(), bm.clone(), lock_table.clone());
            });
            assert!(!t2.is_finished()); // starting new tx is blocked until checkpoint is finished

            drop(tx1);
            t1.join().unwrap();
            thread::sleep(std::time::Duration::from_millis(100)); // HACK
            assert!(t2.is_finished());
        }
    }

    mod nq_ckpt {
        use super::*;

        #[test]
        fn not_blocked() {
            let fm = Arc::new(FileManager::new(
                PathBuf::from("testdata/tx/transaction/nq_ckpt/not_blocked"),
                400,
            ));
            let lm = Arc::new(LogManager::new(fm.clone(), "templog".to_string()));
            let bm = Arc::new(BufferManager::new(fm.clone(), lm.clone(), 8));
            let lock_table = Arc::new(LockTable::new());

            let tx = Transaction::new(fm.clone(), lm.clone(), bm.clone(), lock_table.clone());
            let res = Transaction::nq_ckpt(bm.clone(), lm.clone());

            assert!(res.is_ok());
        }

        #[test]
        fn new_tx_is_kept_waiting() {
            let fm = Arc::new(FileManager::new(
                PathBuf::from("testdata/tx/transaction/nq_ckpt/new_tx_is_kept_waiting"),
                400,
            ));
            let lm = Arc::new(LogManager::new(fm.clone(), "templog".to_string()));
            let bm = Arc::new(BufferManager::new(fm.clone(), lm.clone(), 8));
            let lock_table = Arc::new(LockTable::new());

            let cp_lock = CHECKPOINT_LOCK.lock().unwrap(); // HACK to make nqckpt start waiting
            let t1 = {
                let bm = bm.clone();
                let lm = lm.clone();
                thread::spawn(move || {
                    let res = Transaction::nq_ckpt(bm, lm);
                    assert!(res.is_ok());
                })
            };
            let t2 = thread::spawn(move || {
                let tx = Transaction::new(fm.clone(), lm.clone(), bm.clone(), lock_table.clone());
            });
            assert!(!t2.is_finished()); // starting new tx is blocked until nqckpt is finished

            drop(cp_lock); // start nqckpt
            t1.join().unwrap();
            thread::sleep(std::time::Duration::from_millis(100)); // HACK
            assert!(t2.is_finished());
        }
    }
}
