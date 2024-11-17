#![allow(dead_code)]

#[derive(Eq, PartialEq)]
pub struct Rid {
    block_num: i32,
    slot: i32,
}

impl Rid {
    pub fn new(block_num: i32, slot: i32) -> Self {
        Self { block_num, slot }
    }

    pub fn block_num(&self) -> i32 {
        self.block_num
    }

    pub fn slot(&self) -> i32 {
        self.slot
    }
}

impl std::fmt::Display for Rid {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "[{}, {}]", self.block_num, self.slot)
    }
}
