use std::sync::Mutex;

use crate::{
    file::{block_id::BlockId, file_manager::FileManager, page::Page},
    util::Result,
};

use super::log_iterator::LogIterator;

pub struct LogManager {
    m: Mutex<()>,
    fm: FileManager,
    log_page: Page,
    current_block: BlockId,
    latest_lsn: i32,
    last_saved_lsn: i32,
}

impl LogManager {
    pub fn new(mut fm: FileManager, log_file: String) -> Self {
        let mut log_page = Page::new(fm.block_size());

        let log_size = fm.length(&log_file).unwrap();
        let current_block: BlockId = if log_size == 0 {
            Self::append_new_block(&mut fm, &log_file, &mut log_page).unwrap()
        } else {
            let current_block = BlockId::new(log_file, log_size - 1);
            fm.read(&current_block, &mut log_page).unwrap();
            current_block
        };

        LogManager {
            m: Mutex::new(()),
            fm,
            log_page,
            current_block,
            latest_lsn: 0,
            last_saved_lsn: 0,
        }
    }

    pub fn flush(&mut self, lsn: i32) -> Result<()> {
        if lsn >= self.last_saved_lsn {
            // flush
            self.fm.write(&self.current_block, &self.log_page)?;
            self.last_saved_lsn = self.latest_lsn;
        }
        Ok(())
    }

    pub fn iter(&mut self) -> impl Iterator<Item = Vec<u8>> + '_ {
        // flush
        self.fm.write(&self.current_block, &self.log_page).unwrap();
        self.last_saved_lsn = self.latest_lsn;

        LogIterator::new(&mut self.fm, self.current_block.clone())
    }

    pub fn append(&mut self, log_record: Vec<u8>) -> i32 {
        let _lock = self.m.lock().unwrap();

        let mut boundary = self.log_page.get_int(0).unwrap();
        let bytes_needed = log_record.len() as i32 + 4;
        if boundary - bytes_needed < 4 {
            // flush
            self.fm.write(&self.current_block, &self.log_page).unwrap();
            self.last_saved_lsn = self.latest_lsn;

            self.current_block = Self::append_new_block(
                &mut self.fm,
                self.current_block.filename(),
                &mut self.log_page,
            )
            .unwrap();
            boundary = self.log_page.get_int(0).unwrap();
        }

        let rec_pos = boundary - bytes_needed;
        self.log_page.set_bytes(rec_pos, &log_record);
        self.log_page.set_int(0, rec_pos);

        self.latest_lsn += 1;
        self.latest_lsn
    }

    fn append_new_block(
        fm: &mut FileManager,
        log_file: &str,
        log_page: &mut Page,
    ) -> Result<BlockId> {
        let block = fm.append(&log_file)?;
        log_page.set_int(0, fm.block_size());
        fm.write(&block, &log_page)?;
        Ok(block)
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;

    #[test]
    fn test() {
        let fm = FileManager::new(PathBuf::from("testdata/log/log_manager/test"), 20);
        let mut lm = LogManager::new(fm, "tempfile".to_string());
        assert_eq!(
            std::fs::read("testdata/log/log_manager/test/tempfile").unwrap(),
            vec![0, 0, 0, 20, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0] // append new block
        );

        let lsn1 = lm.append(b"abc".to_vec());
        assert_eq!(
            std::fs::read("testdata/log/log_manager/test/tempfile").unwrap(),
            vec![0, 0, 0, 20, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0] // not flushed yet
        );

        lm.flush(lsn1).unwrap();
        assert_eq!(
            std::fs::read("testdata/log/log_manager/test/tempfile").unwrap(),
            vec![0, 0, 0, 13, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 97, 98, 99] // flushed
        );

        let lsn2 = lm.append(b"def".to_vec());
        assert_eq!(
            std::fs::read("testdata/log/log_manager/test/tempfile").unwrap(),
            vec![0, 0, 0, 13, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 97, 98, 99] // not flushed yet
        );

        lm.flush(lsn2).unwrap();
        assert_eq!(
            std::fs::read("testdata/log/log_manager/test/tempfile").unwrap(),
            vec![0, 0, 0, 6, 0, 0, 0, 0, 0, 3, 100, 101, 102, 0, 0, 0, 3, 97, 98, 99] // flushed
        );

        let lsn3 = lm.append(b"ghi".to_vec());
        assert_eq!(
            std::fs::read("testdata/log/log_manager/test/tempfile").unwrap(),
            vec![
                0, 0, 0, 6, 0, 0, 0, 0, 0, 3, 100, 101, 102, 0, 0, 0, 3, 97, 98, 99, 0, 0, 0, 20,
                0, 0, 0, 0, 0, 3, 100, 101, 102, 0, 0, 0, 3, 97, 98, 99
            ] // append new block
        );

        lm.flush(lsn3).unwrap();
        assert_eq!(
            std::fs::read("testdata/log/log_manager/test/tempfile").unwrap(),
            vec![
                0, 0, 0, 6, 0, 0, 0, 0, 0, 3, 100, 101, 102, 0, 0, 0, 3, 97, 98, 99, 0, 0, 0, 13,
                0, 0, 0, 0, 0, 3, 100, 101, 102, 0, 0, 0, 3, 103, 104, 105
            ] // flushed
        );

        // iterates in reverse order
        let mut iter = lm.iter();
        assert_eq!(iter.next().unwrap(), b"ghi".to_vec());
        assert_eq!(iter.next().unwrap(), b"def".to_vec());
        assert_eq!(iter.next().unwrap(), b"abc".to_vec());
        assert_eq!(iter.next(), None);
    }
}