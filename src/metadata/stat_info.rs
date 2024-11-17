#![allow(dead_code)]

#[derive(Clone)]
pub struct StatInfo {
    num_blocks: i32,
    num_records: i32,
}

impl StatInfo {
    pub fn new(num_blocks: i32, num_records: i32) -> Self {
        Self {
            num_blocks,
            num_records,
        }
    }

    pub fn blocks_accessed(&self) -> i32 {
        self.num_blocks
    }

    pub fn records_output(&self) -> i32 {
        self.num_records
    }

    pub fn distincs_values(&self, _field_name: &str) -> i32 {
        1 + self.num_records / 3
    }
}
