use crate::file::block_id::BlockId;
use crate::file::page::Page;
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
            std::fs::create_dir(&db_directory).unwrap();
        }

        // delete all temp files
        let temp_files = std::fs::read_dir(&db_directory).unwrap();
        for file in temp_files {
            let file = file.unwrap();
            let file_name = file.file_name();
            let file_name = file_name.to_str().unwrap();
            if file_name.starts_with("temp") {
                std::fs::remove_file(file.path()).unwrap();
            }
        }

        FileManager {
            db_directory,
            block_size,
            is_new,
            open_files: HashMap::new(),
        }
    }

    pub fn read(&mut self, block: BlockId, page: &mut Page) {
        let offset = block.block_num() * self.block_size;

        let file = self.get_file(block.filename()).lock().unwrap();
        file.read_exact_at(&mut page.buf, offset as u64).unwrap();
    }

    pub fn write(&mut self, block: BlockId, page: &Page) {
        let offset = block.block_num() * self.block_size;

        let file = self.get_file(block.filename()).lock().unwrap();
        file.write_all_at(&page.buf, offset as u64).unwrap();
        file.sync_all().unwrap();
    }

    pub fn append(&mut self, filename: &str) -> BlockId {
        let block_size = self.block_size;

        let file = self.get_file(filename).lock().unwrap();
        // `new_block_num` must be calculated after the file is locked.
        // `length()` can't be called here because it borrows immutably.
        let new_block_num = FileManager::length_from_file(&file, block_size);
        let new_size = (new_block_num + 1) * block_size;
        file.set_len(new_size as u64).unwrap();
        file.sync_all().unwrap();

        BlockId::new(filename.to_string(), new_block_num)
    }

    fn length_from_file(file: &MutexGuard<File>, block_size: i32) -> i32 {
        file.metadata().unwrap().len() as i32 / block_size
    }

    pub fn length(&mut self, filename: &str) -> i32 {
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
        let mut fm = FileManager::new(PathBuf::from("testdata"), 10);
        let mut page = Page::new(fm.block_size());

        let block = BlockId::new("testfile".to_string(), 1);
        fm.read(block, &mut page);
        assert_eq!(page.get_string(0), "klmnopqrst");
    }

    #[test]
    fn write() {
        let mut fm = FileManager::new(PathBuf::from("testdata"), 10);
        let mut page = Page::new(fm.block_size());

        let block = BlockId::new("tempfile1".to_string(), 1);
        page.set_string(0, "klmnopqrst");
        fm.write(block, &page);

        assert_eq!(
            std::fs::read_to_string("testdata/tempfile1").unwrap(),
            "\0\0\0\0\0\0\0\0\0\0klmnopqrst"
        );
    }

    #[test]
    fn append() {
        let mut fm = FileManager::new(PathBuf::from("testdata"), 10);

        let block = fm.append("tempfile2");
        assert_eq!(block, BlockId::new("tempfile2".to_string(), 0));
        assert_eq!(
            std::fs::read_to_string("testdata/tempfile2").unwrap(),
            "\0\0\0\0\0\0\0\0\0\0"
        );
    }
}
