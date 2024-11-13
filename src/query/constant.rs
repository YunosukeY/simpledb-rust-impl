#![allow(dead_code)]

#[derive(Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum Constant {
    INTEGER(i32),
    STRING(String),
}

impl Constant {
    pub fn as_int(&self) -> Option<i32> {
        match self {
            Constant::INTEGER(i) => Some(*i),
            _ => None,
        }
    }

    pub fn as_string(&self) -> Option<&String> {
        match self {
            Constant::STRING(s) => Some(s),
            _ => None,
        }
    }
}

impl From<i32> for Constant {
    fn from(i: i32) -> Self {
        Constant::INTEGER(i)
    }
}
impl From<String> for Constant {
    fn from(s: String) -> Self {
        Constant::STRING(s)
    }
}

impl std::fmt::Display for Constant {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Constant::INTEGER(i) => write!(f, "{}", i),
            Constant::STRING(s) => write!(f, "{}", s),
        }
    }
}
