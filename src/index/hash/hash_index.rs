#![allow(dead_code)]

use crate::index::index::Index;

pub const NUM_BUCKETS: i32 = 100;

pub struct HashIndex {}

impl HashIndex {
    pub fn new() -> Self {
        Self {}
    }

    pub fn search_cost(num_blocks: i32, _rpb: i32) -> i32 {
        num_blocks / NUM_BUCKETS
    }
}

impl Index for HashIndex {}
