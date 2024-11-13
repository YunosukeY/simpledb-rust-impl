#![allow(dead_code)]

#[derive(Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum Constant {
    Integer(i32),
    String(String),
}

impl Constant {
    pub fn as_int(&self) -> Option<i32> {
        match self {
            Constant::Integer(i) => Some(*i),
            _ => None,
        }
    }

    pub fn as_string(&self) -> Option<&String> {
        match self {
            Constant::String(s) => Some(s),
            _ => None,
        }
    }
}

impl From<i32> for Constant {
    fn from(i: i32) -> Self {
        Constant::Integer(i)
    }
}
impl From<String> for Constant {
    fn from(s: String) -> Self {
        Constant::String(s)
    }
}

impl std::fmt::Display for Constant {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Constant::Integer(i) => write!(f, "{}", i),
            Constant::String(s) => write!(f, "{}", s),
        }
    }
}
