use std::str::from_utf8;

pub struct Page {
    pub(super) buf: Vec<u8>,
}

impl Page {
    pub fn new(size: i32) -> Page {
        Page {
            buf: vec![0; size as usize],
        }
    }

    pub fn from_bytes(bytes: &[u8]) -> Page {
        Page {
            buf: bytes.to_vec(),
        }
    }

    pub fn get_int(&self, offset: i32) -> i32 {
        let ofs = offset as usize;
        let bytes = &self.buf[ofs..ofs + 4];
        let a = bytes.try_into().unwrap();
        i32::from_be_bytes(a)
    }

    pub fn set_int(&mut self, offset: i32, value: i32) {
        let ofs = offset as usize;
        self.buf[ofs..ofs + 4].copy_from_slice(&value.to_be_bytes());
    }

    pub fn get_bytes(&self, offset: i32) -> &[u8] {
        let len = self.get_int(offset);

        let ofs = offset as usize + 4;
        &self.buf[ofs..ofs + len as usize]
    }

    pub fn set_bytes(&mut self, offset: i32, bytes: &[u8]) {
        let len = bytes.len() as i32;
        self.set_int(offset, len);

        let ofs = offset as usize + 4;
        self.buf[ofs..ofs + bytes.len()].copy_from_slice(bytes);
    }

    pub fn get_string(&self, offset: i32) -> String {
        let bytes = self.get_bytes(offset);
        from_utf8(bytes).unwrap().to_string()
    }

    pub fn set_string(&mut self, offset: i32, s: &str) {
        let bytes: &[u8] = s.as_bytes();
        self.set_bytes(offset, bytes);
    }

    pub fn max_length(str: &str) -> i32 {
        str.as_bytes().len() as i32 + 1
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn get_int() {
        let p = Page::from_bytes(&[0, 0, 0, 1, 255, 255, 255, 255, 0, 0, 0, 0]);
        assert_eq!(p.get_int(0), 1);
        assert_eq!(p.get_int(4), -1);
        assert_eq!(p.get_int(8), 0);
    }

    #[test]
    fn set_int() {
        let mut p = Page::new(12);
        p.set_int(0, 1);
        p.set_int(4, -1);

        assert_eq!(p.buf, [0, 0, 0, 1, 255, 255, 255, 255, 0, 0, 0, 0]);
    }

    #[test]
    fn get_bytes() {
        let p = Page::from_bytes(&[0, 0, 0, 0, 3, 1, 2, 3, 0, 0]);
        assert_eq!(p.get_bytes(1), &[1, 2, 3]);
    }

    #[test]
    fn set_bytes() {
        let mut p = Page::new(10);
        p.set_bytes(1, &[1, 2, 3]);
        assert_eq!(p.buf, [0, 0, 0, 0, 3, 1, 2, 3, 0, 0]);
    }

    #[test]
    fn get_string() {
        let p = Page::from_bytes(&[0, 0, 0, 0, 3, 97, 98, 99, 0, 0]);
        assert_eq!(p.get_string(1), "abc");
    }

    #[test]
    fn set_string() {
        let mut p = Page::new(10);
        p.set_string(1, "abc");
        assert_eq!(p.buf, [0, 0, 0, 0, 3, 97, 98, 99, 0, 0]);
    }
}
