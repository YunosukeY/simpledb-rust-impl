#![allow(dead_code)]

use crate::file::page::Page;
use crate::{file::block_id::BlockId, util::Result};
use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    os::unix::fs::FileExt,
    path::PathBuf,
    sync::{Mutex, MutexGuard},
};

pub struct FileManager {
    db_directory: PathBuf,
    block_size: i32,
    is_new: bool,
    open_files: HashMap<String, Mutex<File>>,
}

impl FileManager {
    pub fn new(db_directory: PathBuf, block_size: i32) -> FileManager {
        let is_new = !db_directory.exists();
        // create directory if it doesn't exist
        if is_new {
            std::fs::create_dir_all(&db_directory).unwrap();
        }

        // delete all temp files
        let temp_files = std::fs::read_dir(&db_directory).unwrap();
        for file in temp_files {
            if file.is_err() {
                continue;
            }
            let file = file.unwrap();

            let file_name = file.file_name().into_string();
            if file_name.is_err() {
                continue;
            }
            if file_name.unwrap().starts_with("temp") {
                let _ = std::fs::remove_file(file.path());
            }
        }

        FileManager {
            db_directory,
            block_size,
            is_new,
            open_files: HashMap::new(),
        }
    }

    pub fn read(&mut self, block: &BlockId, page: &mut Page) -> Result<()> {
        let offset = block.block_num() * self.block_size;

        let file = self.get_file(block.filename()).lock().unwrap();
        file.read_exact_at(&mut page.buf, offset as u64)?;
        Ok(())
    }

    pub fn write(&mut self, block: &BlockId, page: &Page) -> Result<()> {
        let offset = block.block_num() * self.block_size;

        let file = self.get_file(block.filename()).lock().unwrap();
        file.write_all_at(&page.buf, offset as u64)?;
        file.sync_all()?;
        Ok(())
    }

    pub fn append(&mut self, filename: &str) -> Result<BlockId> {
        let block_size = self.block_size;

        let file = self.get_file(filename).lock().unwrap();
        // `new_block_num` must be calculated after the file is locked.
        // `length()` can't be called here because it borrows immutably.
        let new_block_num = FileManager::length_from_file(&file, block_size)?;
        let new_size = (new_block_num + 1) * block_size;
        file.set_len(new_size as u64)?;
        file.sync_all()?;

        Ok(BlockId::new(filename.to_string(), new_block_num))
    }

    fn length_from_file(file: &MutexGuard<File>, block_size: i32) -> Result<i32> {
        Ok(file.metadata()?.len() as i32 / block_size)
    }

    pub fn length(&mut self, filename: &str) -> Result<i32> {
        let block_size = self.block_size;

        let file = self.get_file(filename).lock().unwrap();
        FileManager::length_from_file(&file, block_size)
    }

    pub fn is_new(&self) -> bool {
        self.is_new
    }

    pub fn block_size(&self) -> i32 {
        self.block_size
    }

    fn get_file(&mut self, filename: &str) -> &Mutex<File> {
        if !self.open_files.contains_key(filename) {
            let path = self.db_directory.join(filename);
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(path)
                .unwrap();
            let mutex = Mutex::new(file);
            self.open_files.insert(filename.to_string(), mutex);
        }
        self.open_files.get(filename).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read() {
        let mut fm = FileManager::new(PathBuf::from("testdata/file/file_manager/read"), 10);
        let mut page = Page::new(fm.block_size());

        let block = BlockId::new("testfile".to_string(), 1);
        fm.read(&block, &mut page).unwrap();
        assert_eq!(page.get_string(0), "abc");
    }

    #[test]
    fn write() {
        let mut fm = FileManager::new(PathBuf::from("testdata/file/file_manager/write"), 10);
        let mut page = Page::new(fm.block_size());

        let block = BlockId::new("tempfile1".to_string(), 1);
        page.set_string(0, "abc");
        fm.write(&block, &page).unwrap();

        assert_eq!(
            std::fs::read_to_string("testdata/file/file_manager/write/tempfile1").unwrap(),
            "\0\0\0\0\0\0\0\0\0\0\0\0\0\u{3}abc\0\0\0"
        );
    }

    #[test]
    fn append() {
        let mut fm = FileManager::new(PathBuf::from("testdata/file/file_manager/append"), 10);

        let block = fm.append("tempfile2").unwrap();
        assert_eq!(block, BlockId::new("tempfile2".to_string(), 0));
        assert_eq!(
            std::fs::read_to_string("testdata/file/file_manager/append/tempfile2").unwrap(),
            "\0\0\0\0\0\0\0\0\0\0"
        );
    }
}
