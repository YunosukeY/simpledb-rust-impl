#[derive(Debug, Hash, Eq, PartialEq)]
pub struct BlockId {
    filename: String,
    block_num: u32,
}

impl BlockId {
    pub fn new(filename: String, block_num: u32) -> BlockId {
        BlockId {
            filename,
            block_num,
        }
    }

    pub fn filename(&self) -> &str {
        &self.filename
    }

    pub fn block_num(&self) -> u32 {
        self.block_num
    }
}
