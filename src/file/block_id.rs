#[derive(Debug, Hash, Eq, PartialEq, Clone)]
pub struct BlockId {
    filename: String,
    block_num: i32,
}

impl BlockId {
    pub fn new(filename: String, block_num: i32) -> BlockId {
        BlockId {
            filename,
            block_num,
        }
    }

    pub fn filename(&self) -> &str {
        &self.filename
    }

    pub fn block_num(&self) -> i32 {
        self.block_num
    }
}

impl std::fmt::Display for BlockId {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "[file {}, block {}]", self.filename, self.block_num)
    }
}
