pub struct Page {
    pub(super) buf: Vec<u8>,
}

impl Page {
    pub fn new(size: usize) -> Page {
        Page { buf: vec![0; size] }
    }

    pub fn from_bytes(bytes: &[u8]) -> Page {
        Page {
            buf: bytes.to_vec(),
        }
    }

    pub fn get_int(&self, offset: usize) -> i32 {
        let bytes = &self.buf[offset..offset + 4];
        i32::from_be_bytes(bytes.try_into().unwrap())
    }

    pub fn set_int(&mut self, offset: usize, value: i32) {
        self.buf[offset..offset + 4].copy_from_slice(&value.to_be_bytes());
    }

    pub fn get_bytes(&self, offset: usize) -> &[u8] {
        &self.buf[offset..]
    }

    pub fn set_bytes(&mut self, offset: usize, bytes: &[u8]) {
        self.buf[offset..offset + bytes.len()].copy_from_slice(bytes);
    }

    pub fn get_string(&self, offset: usize) -> String {
        std::str::from_utf8(&self.buf[offset..])
            .unwrap()
            .to_string()
    }

    pub fn set_string(&mut self, offset: usize, s: &str) {
        let bytes: &[u8] = s.as_bytes();
        self.set_bytes(offset, bytes);
    }

    pub fn max_length(str: &str) -> usize {
        str.as_bytes().len() + 1
    }
}
