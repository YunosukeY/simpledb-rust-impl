#![allow(dead_code)]
#![allow(unused_variables)]

use std::sync::{Arc, Mutex};

use crate::{
    buffer::buffer_manager::BufferManager,
    file::{block_id::BlockId, file_manager::FileManager},
    log::log_manager::LogManager,
};

use super::{
    buffer_list::BufferList,
    concurrency::{concurrency_manager::ConcurrencyManager, lock_table::LockTable},
    recovery::recovery_manager::RecoveryManager,
};

static NEXT_TX_NUM: Mutex<i32> = Mutex::new(0);
const END_OF_FILE: i32 = -1;

pub struct Transaction {
    rm: RecoveryManager,
    cm: ConcurrencyManager,
    bm: Arc<BufferManager>,
    fm: Arc<FileManager>,
    tx_num: i32,
    my_buffers: BufferList,
}

impl Transaction {
    pub fn new(
        fm: Arc<FileManager>,
        lm: Arc<LogManager>,
        bm: BufferManager,
        lock_table: Arc<LockTable>,
    ) -> Self {
        let tx_num = Self::next_tx_number();
        let bm = Arc::new(bm);
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
        }
    }

    pub fn commit(&mut self) {
        self.rm.commit();
        // TODO log
        self.cm.release();
        self.my_buffers.unpin_all();
    }

    pub fn rollback(&mut self) {
        let rm = &mut self.rm as *mut RecoveryManager;
        unsafe {
            (*rm).rollback(self);
        }
        // TODO log
        self.cm.release();
        self.my_buffers.unpin_all();
    }

    pub fn recover(&mut self) {
        let bm = Arc::as_ptr(&self.bm) as *mut BufferManager;
        unsafe {
            (*bm).flush_all(self.tx_num).unwrap();
        }
        let rm = &mut self.rm as *mut RecoveryManager;
        unsafe {
            (*rm).recover(self);
        }
    }

    pub fn pin(&mut self, block: &BlockId) {
        self.my_buffers.pin(block.clone()).unwrap();
    }

    pub fn unpin(&mut self, block: &BlockId) {
        self.my_buffers.unpin(block.clone());
    }

    pub fn get_int(&mut self, block: &BlockId, offset: i32) -> i32 {
        self.cm.s_lock(block).unwrap();
        let buffer = self.my_buffers.buffer(block).unwrap();
        buffer.contents.get_int(offset)
    }
    pub fn set_int(&mut self, block: &BlockId, offset: i32, value: i32, log: bool) {
        self.cm.x_lock(block).unwrap();
        let buffer = self.my_buffers.buffer_mut(block).unwrap();
        let mut lsn = -1;
        if log {
            lsn = self.rm.set_int(buffer, offset, value);
        }
        buffer.contents.set_int(offset, value);
        buffer.set_modified(self.tx_num, lsn);
    }

    pub fn get_bytes(&mut self, block: &BlockId, offset: i32) -> &[u8] {
        self.cm.s_lock(block).unwrap();
        let buffer = self.my_buffers.buffer(block).unwrap();
        buffer.contents.get_bytes(offset)
    }
    pub fn set_bytes(&mut self, block: &BlockId, offset: i32, value: &[u8], log: bool) {
        self.cm.x_lock(block).unwrap();
        let buffer = self.my_buffers.buffer_mut(block).unwrap();
        let mut lsn = -1;
        if log {
            lsn = self.rm.set_bytes(buffer, offset, value);
        }
        buffer.contents.set_bytes(offset, value);
        buffer.set_modified(self.tx_num, lsn);
    }

    pub fn get_string(&mut self, block: &BlockId, offset: i32) -> String {
        self.cm.s_lock(block).unwrap();
        let buffer = self.my_buffers.buffer(block).unwrap();
        buffer.contents.get_string(offset)
    }
    pub fn set_string(&mut self, block: &BlockId, offset: i32, value: &str, log: bool) {
        self.cm.x_lock(block).unwrap();
        let buffer = self.my_buffers.buffer_mut(block).unwrap();
        let mut lsn = -1;
        if log {
            lsn = self.rm.set_string(buffer, offset, value);
        }
        buffer.contents.set_string(offset, value);
        buffer.set_modified(self.tx_num, lsn);
    }

    pub fn get_bool(&mut self, block: &BlockId, offset: i32) -> bool {
        self.cm.s_lock(block).unwrap();
        let buffer = self.my_buffers.buffer(block).unwrap();
        buffer.contents.get_bool(offset)
    }
    pub fn set_bool(&mut self, block: &BlockId, offset: i32, value: bool, log: bool) {
        self.cm.x_lock(block).unwrap();
        let buffer = self.my_buffers.buffer_mut(block).unwrap();
        let mut lsn = -1;
        if log {
            lsn = self.rm.set_bool(buffer, offset, value);
        }
        buffer.contents.set_bool(offset, value);
        buffer.set_modified(self.tx_num, lsn);
    }

    pub fn get_double(&mut self, block: &BlockId, offset: i32) -> f64 {
        self.cm.s_lock(block).unwrap();
        let buffer = self.my_buffers.buffer(block).unwrap();
        buffer.contents.get_double(offset)
    }
    pub fn set_double(&mut self, block: &BlockId, offset: i32, value: f64, log: bool) {
        self.cm.x_lock(block).unwrap();
        let buffer = self.my_buffers.buffer_mut(block).unwrap();
        let mut lsn = -1;
        if log {
            lsn = self.rm.set_double(buffer, offset, value);
        }
        buffer.contents.set_double(offset, value);
        buffer.set_modified(self.tx_num, lsn);
    }

    pub fn get_date(&mut self, block: &BlockId, offset: i32) -> Option<chrono::NaiveDate> {
        self.cm.s_lock(block).unwrap();
        let buffer = self.my_buffers.buffer(block).unwrap();
        buffer.contents.get_date(offset)
    }
    pub fn set_date(
        &mut self,
        block: &BlockId,
        offset: i32,
        value: &Option<chrono::NaiveDate>,
        log: bool,
    ) {
        self.cm.x_lock(block).unwrap();
        let buffer = self.my_buffers.buffer_mut(block).unwrap();
        let mut lsn = -1;
        if log {
            lsn = self.rm.set_date(buffer, offset, value);
        }
        buffer.contents.set_date(offset, value);
        buffer.set_modified(self.tx_num, lsn);
    }

    pub fn get_time(&mut self, block: &BlockId, offset: i32) -> Option<chrono::NaiveTime> {
        self.cm.s_lock(block).unwrap();
        let buffer = self.my_buffers.buffer(block).unwrap();
        buffer.contents.get_time(offset)
    }
    pub fn set_time(
        &mut self,
        block: &BlockId,
        offset: i32,
        value: &Option<chrono::NaiveTime>,
        log: bool,
    ) {
        self.cm.x_lock(block).unwrap();
        let buffer = self.my_buffers.buffer_mut(block).unwrap();
        let mut lsn = -1;
        if log {
            lsn = self.rm.set_time(buffer, offset, value);
        }
        buffer.contents.set_time(offset, value);
        buffer.set_modified(self.tx_num, lsn);
    }

    pub fn get_datetime(
        &mut self,
        block: &BlockId,
        offset: i32,
    ) -> Option<chrono::DateTime<chrono::FixedOffset>> {
        self.cm.s_lock(block).unwrap();
        let buffer = self.my_buffers.buffer(block).unwrap();
        buffer.contents.get_datetime(offset)
    }
    pub fn set_datetime(
        &mut self,
        block: &BlockId,
        offset: i32,
        value: &Option<chrono::DateTime<chrono::FixedOffset>>,
        log: bool,
    ) {
        self.cm.x_lock(block).unwrap();
        let buffer = self.my_buffers.buffer_mut(block).unwrap();
        let mut lsn = -1;
        if log {
            lsn = self.rm.set_datetime(buffer, offset, value);
        }
        buffer.contents.set_datetime(offset, value);
        buffer.set_modified(self.tx_num, lsn);
    }

    pub fn get_json(&mut self, block: &BlockId, offset: i32) -> Option<serde_json::Value> {
        self.cm.s_lock(block).unwrap();
        let buffer = self.my_buffers.buffer(block).unwrap();
        buffer.contents.get_json(offset)
    }
    pub fn set_json(
        &mut self,
        block: &BlockId,
        offset: i32,
        value: &Option<serde_json::Value>,
        log: bool,
    ) {
        self.cm.x_lock(block).unwrap();
        let buffer = self.my_buffers.buffer_mut(block).unwrap();
        let mut lsn = -1;
        if log {
            lsn = self.rm.set_json(buffer, offset, value);
        }
        buffer.contents.set_json(offset, value);
        buffer.set_modified(self.tx_num, lsn);
    }

    pub fn size(&mut self, filename: &str) -> i32 {
        let dummy = BlockId::new(filename.to_string(), END_OF_FILE);
        self.cm.x_lock(&dummy).unwrap();
        let fm = Arc::as_ptr(&self.fm) as *mut FileManager;
        unsafe { (*fm).length(filename).unwrap() }
    }

    pub fn append(&mut self, filename: &str) -> BlockId {
        let dummy = BlockId::new(filename.to_string(), END_OF_FILE);
        self.cm.x_lock(&dummy).unwrap();
        let fm = Arc::as_ptr(&self.fm) as *mut FileManager;
        unsafe { (*fm).append(filename).unwrap() }
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
            let bm = BufferManager::new(fm.clone(), lm.clone(), 8);
            let mut tx = Transaction::new(fm.clone(), lm.clone(), bm, Arc::new(LockTable::new()));
            let block = BlockId::new("tempfile".to_string(), 0);

            tx.pin(&block);
            tx.set_int(&block, 0, 123, true);

            assert_eq!(tx.get_int(&block, 0), 123);
        }

        #[test]
        fn set_but_rollback() {
            let fm = Arc::new(FileManager::new(
                PathBuf::from("testdata/tx/transaction/int/commit_but_rollback"),
                400,
            ));
            let lm = Arc::new(LogManager::new(fm.clone(), "templog".to_string()));
            let bm = BufferManager::new(fm.clone(), lm.clone(), 8);
            let mut tx = Transaction::new(fm.clone(), lm.clone(), bm, Arc::new(LockTable::new()));
            let block = BlockId::new("tempfile".to_string(), 0);

            tx.pin(&block);
            tx.set_int(&block, 0, 123, true);
            tx.rollback();

            tx.pin(&block);
            assert_eq!(tx.get_int(&block, 0), 0);
        }

        #[test]
        fn commit_and_recover() {
            let fm = Arc::new(FileManager::new(
                PathBuf::from("testdata/tx/transaction/int/commit_and_recover"),
                400,
            ));
            let lm = Arc::new(LogManager::new(fm.clone(), "templog".to_string()));
            let bm = BufferManager::new(fm.clone(), lm.clone(), 8);
            let mut tx = Transaction::new(fm.clone(), lm.clone(), bm, Arc::new(LockTable::new()));
            let block = BlockId::new("tempfile".to_string(), 0);

            tx.pin(&block);
            tx.set_int(&block, 0, 123, true);
            tx.commit();
            tx.recover();

            tx.pin(&block);
            assert_eq!(tx.get_int(&block, 0), 123);
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
            let bm = BufferManager::new(fm.clone(), lm.clone(), 8);
            let mut tx = Transaction::new(fm.clone(), lm.clone(), bm, Arc::new(LockTable::new()));
            let block = BlockId::new("tempfile".to_string(), 0);

            tx.pin(&block);
            tx.set_bytes(&block, 0, &[1, 2, 3], true);

            assert_eq!(tx.get_bytes(&block, 0), &[1, 2, 3]);
        }

        #[test]
        fn set_but_rollback() {
            let fm = Arc::new(FileManager::new(
                PathBuf::from("testdata/tx/transaction/bytes/set_but_rollback"),
                400,
            ));
            let lm = Arc::new(LogManager::new(fm.clone(), "templog".to_string()));
            let bm = BufferManager::new(fm.clone(), lm.clone(), 8);
            let mut tx = Transaction::new(fm.clone(), lm.clone(), bm, Arc::new(LockTable::new()));
            let block = BlockId::new("tempfile".to_string(), 0);

            tx.pin(&block);
            tx.set_bytes(&block, 0, &[1, 2, 3], true);
            tx.rollback();

            tx.pin(&block);
            assert_eq!(tx.get_bytes(&block, 0), &[] as &[u8]);
        }

        #[test]
        fn commit_and_recover() {
            let fm = Arc::new(FileManager::new(
                PathBuf::from("testdata/tx/transaction/bytes/commit_and_recover"),
                400,
            ));
            let lm = Arc::new(LogManager::new(fm.clone(), "templog".to_string()));
            let bm = BufferManager::new(fm.clone(), lm.clone(), 8);
            let mut tx = Transaction::new(fm.clone(), lm.clone(), bm, Arc::new(LockTable::new()));
            let block = BlockId::new("tempfile".to_string(), 0);

            tx.pin(&block);
            tx.set_bytes(&block, 0, &[1, 2, 3], true);
            tx.commit();
            tx.recover();

            tx.pin(&block);
            assert_eq!(tx.get_bytes(&block, 0), &[1, 2, 3]);
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
            let bm = BufferManager::new(fm.clone(), lm.clone(), 8);
            let mut tx = Transaction::new(fm.clone(), lm.clone(), bm, Arc::new(LockTable::new()));
            let block = BlockId::new("tempfile".to_string(), 0);

            tx.pin(&block);
            tx.set_string(&block, 0, "abc", true);

            assert_eq!(tx.get_string(&block, 0), "abc");
        }

        #[test]
        fn set_but_rollback() {
            let fm = Arc::new(FileManager::new(
                PathBuf::from("testdata/tx/transaction/string/set_but_rollback"),
                400,
            ));
            let lm = Arc::new(LogManager::new(fm.clone(), "templog".to_string()));
            let bm = BufferManager::new(fm.clone(), lm.clone(), 8);
            let mut tx = Transaction::new(fm.clone(), lm.clone(), bm, Arc::new(LockTable::new()));
            let block = BlockId::new("tempfile".to_string(), 0);

            tx.pin(&block);
            tx.set_string(&block, 0, "abc", true);
            tx.rollback();

            tx.pin(&block);
            assert_eq!(tx.get_string(&block, 0), "");
        }

        #[test]
        fn commit_and_recover() {
            let fm = Arc::new(FileManager::new(
                PathBuf::from("testdata/tx/transaction/string/commit_and_recover"),
                400,
            ));
            let lm = Arc::new(LogManager::new(fm.clone(), "templog".to_string()));
            let bm = BufferManager::new(fm.clone(), lm.clone(), 8);
            let mut tx = Transaction::new(fm.clone(), lm.clone(), bm, Arc::new(LockTable::new()));
            let block = BlockId::new("tempfile".to_string(), 0);

            tx.pin(&block);
            tx.set_string(&block, 0, "abc", true);
            tx.commit();
            tx.recover();

            tx.pin(&block);
            assert_eq!(tx.get_string(&block, 0), "abc");
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
            let bm = BufferManager::new(fm.clone(), lm.clone(), 8);
            let mut tx = Transaction::new(fm.clone(), lm.clone(), bm, Arc::new(LockTable::new()));
            let block = BlockId::new("tempfile".to_string(), 0);

            tx.pin(&block);
            tx.set_bool(&block, 0, true, true);

            assert_eq!(tx.get_bool(&block, 0), true);
        }

        #[test]
        fn set_but_rollback() {
            let fm = Arc::new(FileManager::new(
                PathBuf::from("testdata/tx/transaction/bool/set_but_rollback"),
                400,
            ));
            let lm = Arc::new(LogManager::new(fm.clone(), "templog".to_string()));
            let bm = BufferManager::new(fm.clone(), lm.clone(), 8);
            let mut tx = Transaction::new(fm.clone(), lm.clone(), bm, Arc::new(LockTable::new()));
            let block = BlockId::new("tempfile".to_string(), 0);

            tx.pin(&block);
            tx.set_bool(&block, 0, true, true);
            tx.rollback();

            tx.pin(&block);
            assert_eq!(tx.get_bool(&block, 0), false);
        }

        #[test]
        fn commit_and_recover() {
            let fm = Arc::new(FileManager::new(
                PathBuf::from("testdata/tx/transaction/bool/commit_and_recover"),
                400,
            ));
            let lm = Arc::new(LogManager::new(fm.clone(), "templog".to_string()));
            let bm = BufferManager::new(fm.clone(), lm.clone(), 8);
            let mut tx = Transaction::new(fm.clone(), lm.clone(), bm, Arc::new(LockTable::new()));
            let block = BlockId::new("tempfile".to_string(), 0);

            tx.pin(&block);
            tx.set_bool(&block, 0, true, true);
            tx.commit();
            tx.recover();

            tx.pin(&block);
            assert_eq!(tx.get_bool(&block, 0), true);
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
            let bm = BufferManager::new(fm.clone(), lm.clone(), 8);
            let mut tx = Transaction::new(fm.clone(), lm.clone(), bm, Arc::new(LockTable::new()));
            let block = BlockId::new("tempfile".to_string(), 0);

            tx.pin(&block);
            tx.set_double(&block, 0, 1.23, true);

            assert_eq!(tx.get_double(&block, 0), 1.23);
        }

        #[test]
        fn set_but_rollback() {
            let fm = Arc::new(FileManager::new(
                PathBuf::from("testdata/tx/transaction/double/set_but_rollback"),
                400,
            ));
            let lm = Arc::new(LogManager::new(fm.clone(), "templog".to_string()));
            let bm = BufferManager::new(fm.clone(), lm.clone(), 8);
            let mut tx = Transaction::new(fm.clone(), lm.clone(), bm, Arc::new(LockTable::new()));
            let block = BlockId::new("tempfile".to_string(), 0);

            tx.pin(&block);
            tx.set_double(&block, 0, 1.23, true);
            tx.rollback();

            tx.pin(&block);
            assert_eq!(tx.get_double(&block, 0), 0.0);
        }

        #[test]
        fn commit_and_recover() {
            let fm = Arc::new(FileManager::new(
                PathBuf::from("testdata/tx/transaction/double/commit_and_recover"),
                400,
            ));
            let lm = Arc::new(LogManager::new(fm.clone(), "templog".to_string()));
            let bm = BufferManager::new(fm.clone(), lm.clone(), 8);
            let mut tx = Transaction::new(fm.clone(), lm.clone(), bm, Arc::new(LockTable::new()));
            let block = BlockId::new("tempfile".to_string(), 0);

            tx.pin(&block);
            tx.set_double(&block, 0, 1.23, true);
            tx.commit();
            tx.recover();

            tx.pin(&block);
            assert_eq!(tx.get_double(&block, 0), 1.23);
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
            let bm = BufferManager::new(fm.clone(), lm.clone(), 8);
            let mut tx = Transaction::new(fm.clone(), lm.clone(), bm, Arc::new(LockTable::new()));
            let block = BlockId::new("tempfile".to_string(), 0);

            tx.pin(&block);
            tx.set_date(
                &block,
                0,
                &chrono::NaiveDate::from_ymd_opt(2021, 1, 1),
                true,
            );

            assert_eq!(
                tx.get_date(&block, 0),
                chrono::NaiveDate::from_ymd_opt(2021, 1, 1)
            );
        }

        #[test]
        fn set_but_rollback() {
            let fm = Arc::new(FileManager::new(
                PathBuf::from("testdata/tx/transaction/date/set_but_rollback"),
                400,
            ));
            let lm = Arc::new(LogManager::new(fm.clone(), "templog".to_string()));
            let bm = BufferManager::new(fm.clone(), lm.clone(), 8);
            let mut tx = Transaction::new(fm.clone(), lm.clone(), bm, Arc::new(LockTable::new()));
            let block = BlockId::new("tempfile".to_string(), 0);

            tx.pin(&block);
            tx.set_date(
                &block,
                0,
                &chrono::NaiveDate::from_ymd_opt(2021, 1, 1),
                true,
            );
            tx.rollback();

            tx.pin(&block);
            assert_eq!(tx.get_date(&block, 0), None);
        }

        #[test]
        fn commit_and_recover() {
            let fm = Arc::new(FileManager::new(
                PathBuf::from("testdata/tx/transaction/date/commit_and_recover"),
                400,
            ));
            let lm = Arc::new(LogManager::new(fm.clone(), "templog".to_string()));
            let bm = BufferManager::new(fm.clone(), lm.clone(), 8);
            let mut tx = Transaction::new(fm.clone(), lm.clone(), bm, Arc::new(LockTable::new()));
            let block = BlockId::new("tempfile".to_string(), 0);

            tx.pin(&block);
            tx.set_date(
                &block,
                0,
                &chrono::NaiveDate::from_ymd_opt(2021, 1, 1),
                true,
            );
            tx.commit();
            tx.recover();

            tx.pin(&block);
            assert_eq!(
                tx.get_date(&block, 0),
                chrono::NaiveDate::from_ymd_opt(2021, 1, 1)
            );
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
            let bm = BufferManager::new(fm.clone(), lm.clone(), 8);
            let mut tx = Transaction::new(fm.clone(), lm.clone(), bm, Arc::new(LockTable::new()));
            let block = BlockId::new("tempfile".to_string(), 0);

            tx.pin(&block);
            tx.set_time(
                &block,
                0,
                &chrono::NaiveTime::from_hms_opt(12, 34, 56),
                true,
            );

            assert_eq!(
                tx.get_time(&block, 0),
                chrono::NaiveTime::from_hms_opt(12, 34, 56)
            );
        }

        #[test]
        fn set_but_rollback() {
            let fm = Arc::new(FileManager::new(
                PathBuf::from("testdata/tx/transaction/time/set_but_rollback"),
                400,
            ));
            let lm = Arc::new(LogManager::new(fm.clone(), "templog".to_string()));
            let bm = BufferManager::new(fm.clone(), lm.clone(), 8);
            let mut tx = Transaction::new(fm.clone(), lm.clone(), bm, Arc::new(LockTable::new()));
            let block = BlockId::new("tempfile".to_string(), 0);

            tx.pin(&block);
            tx.set_time(
                &block,
                0,
                &chrono::NaiveTime::from_hms_opt(12, 34, 56),
                true,
            );
            tx.rollback();

            tx.pin(&block);
            assert_eq!(
                tx.get_time(&block, 0),
                chrono::NaiveTime::from_hms_opt(0, 0, 0)
            );
        }

        #[test]
        fn commit_and_recover() {
            let fm = Arc::new(FileManager::new(
                PathBuf::from("testdata/tx/transaction/time/commit_and_recover"),
                400,
            ));
            let lm = Arc::new(LogManager::new(fm.clone(), "templog".to_string()));
            let bm = BufferManager::new(fm.clone(), lm.clone(), 8);
            let mut tx = Transaction::new(fm.clone(), lm.clone(), bm, Arc::new(LockTable::new()));
            let block = BlockId::new("tempfile".to_string(), 0);

            tx.pin(&block);
            tx.set_time(
                &block,
                0,
                &chrono::NaiveTime::from_hms_opt(12, 34, 56),
                true,
            );
            tx.commit();
            tx.recover();

            tx.pin(&block);
            assert_eq!(
                tx.get_time(&block, 0),
                chrono::NaiveTime::from_hms_opt(12, 34, 56)
            );
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
            let bm = BufferManager::new(fm.clone(), lm.clone(), 8);
            let mut tx = Transaction::new(fm.clone(), lm.clone(), bm, Arc::new(LockTable::new()));
            let block = BlockId::new("tempfile".to_string(), 0);
            let now = chrono::Utc::now().fixed_offset();

            tx.pin(&block);
            tx.set_datetime(&block, 0, &Some(now), true);

            assert_eq!(tx.get_datetime(&block, 0).unwrap(), now);
        }

        #[test]
        fn set_but_rollback() {
            let fm = Arc::new(FileManager::new(
                PathBuf::from("testdata/tx/transaction/datetime/set_but_rollback"),
                400,
            ));
            let lm = Arc::new(LogManager::new(fm.clone(), "templog".to_string()));
            let bm = BufferManager::new(fm.clone(), lm.clone(), 8);
            let mut tx = Transaction::new(fm.clone(), lm.clone(), bm, Arc::new(LockTable::new()));
            let block = BlockId::new("tempfile".to_string(), 0);
            let now = chrono::Utc::now().fixed_offset();

            tx.pin(&block);
            tx.set_datetime(&block, 0, &Some(now), true);
            tx.rollback();

            tx.pin(&block);
            assert_eq!(tx.get_datetime(&block, 0), None);
        }

        #[test]
        fn commit_and_recover() {
            let fm = Arc::new(FileManager::new(
                PathBuf::from("testdata/tx/transaction/datetime/commit_and_recover"),
                400,
            ));
            let lm = Arc::new(LogManager::new(fm.clone(), "templog".to_string()));
            let bm = BufferManager::new(fm.clone(), lm.clone(), 8);
            let mut tx = Transaction::new(fm.clone(), lm.clone(), bm, Arc::new(LockTable::new()));
            let block = BlockId::new("tempfile".to_string(), 0);
            let now = chrono::Utc::now().fixed_offset();

            tx.pin(&block);
            tx.set_datetime(&block, 0, &Some(now), true);
            tx.commit();
            tx.recover();

            tx.pin(&block);
            assert_eq!(tx.get_datetime(&block, 0).unwrap(), now);
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
            let bm = BufferManager::new(fm.clone(), lm.clone(), 8);
            let mut tx = Transaction::new(fm.clone(), lm.clone(), bm, Arc::new(LockTable::new()));
            let block = BlockId::new("tempfile".to_string(), 0);

            tx.pin(&block);
            tx.set_json(&block, 0, &Some(serde_json::json!({"key": "value"})), true);

            assert_eq!(
                tx.get_json(&block, 0).unwrap(),
                serde_json::json!({"key": "value"})
            );
        }

        #[test]
        fn set_but_rollback() {
            let fm = Arc::new(FileManager::new(
                PathBuf::from("testdata/tx/transaction/json/set_but_rollback"),
                400,
            ));
            let lm = Arc::new(LogManager::new(fm.clone(), "templog".to_string()));
            let bm = BufferManager::new(fm.clone(), lm.clone(), 8);
            let mut tx = Transaction::new(fm.clone(), lm.clone(), bm, Arc::new(LockTable::new()));
            let block = BlockId::new("tempfile".to_string(), 0);

            tx.pin(&block);
            tx.set_json(&block, 0, &Some(serde_json::json!({"key": "value"})), true);
            tx.rollback();

            tx.pin(&block);
            assert_eq!(tx.get_json(&block, 0), None);
        }

        #[test]
        fn commit_and_recover() {
            let fm = Arc::new(FileManager::new(
                PathBuf::from("testdata/tx/transaction/json/commit_and_recover"),
                400,
            ));
            let lm = Arc::new(LogManager::new(fm.clone(), "templog".to_string()));
            let bm = BufferManager::new(fm.clone(), lm.clone(), 8);
            let mut tx = Transaction::new(fm.clone(), lm.clone(), bm, Arc::new(LockTable::new()));
            let block = BlockId::new("tempfile".to_string(), 0);

            tx.pin(&block);
            tx.set_json(&block, 0, &Some(serde_json::json!({"key": "value"})), true);
            tx.commit();
            tx.recover();

            tx.pin(&block);
            assert_eq!(
                tx.get_json(&block, 0).unwrap(),
                serde_json::json!({"key": "value"})
            );
        }
    }
}
