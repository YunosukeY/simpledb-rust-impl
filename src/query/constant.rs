#![allow(dead_code)]

use chrono::{DateTime, FixedOffset, NaiveDate, NaiveTime};

#[derive(PartialEq)]
pub enum Constant {
    Integer(i32),
    Double(f64),
    Bytes(Vec<u8>),
    String(String),
    Bool(bool),
    Date(NaiveDate),
    Time(NaiveTime),
    DateTime(DateTime<FixedOffset>),
    Json(serde_json::Value),
}

impl PartialOrd for Constant {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match (self, other) {
            (Constant::Integer(i1), Constant::Integer(i2)) => i1.partial_cmp(i2),
            (Constant::Double(d1), Constant::Double(d2)) => d1.partial_cmp(d2),
            (Constant::Bytes(b1), Constant::Bytes(b2)) => b1.partial_cmp(b2),
            (Constant::String(s1), Constant::String(s2)) => s1.partial_cmp(s2),
            (Constant::Bool(b1), Constant::Bool(b2)) => b1.partial_cmp(b2),
            (Constant::Date(d1), Constant::Date(d2)) => d1.partial_cmp(d2),
            (Constant::Time(t1), Constant::Time(t2)) => t1.partial_cmp(t2),
            (Constant::DateTime(dt1), Constant::DateTime(dt2)) => dt1.partial_cmp(dt2),
            _ => None,
        }
    }
}

impl Constant {
    pub fn as_int(&self) -> Option<i32> {
        match self {
            Constant::Integer(i) => Some(*i),
            _ => None,
        }
    }

    pub fn as_double(&self) -> Option<f64> {
        match self {
            Constant::Double(d) => Some(*d),
            _ => None,
        }
    }

    pub fn as_bytes(&self) -> Option<&Vec<u8>> {
        match self {
            Constant::Bytes(b) => Some(b),
            _ => None,
        }
    }

    pub fn as_string(&self) -> Option<&String> {
        match self {
            Constant::String(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_boolean(&self) -> Option<bool> {
        match self {
            Constant::Bool(b) => Some(*b),
            _ => None,
        }
    }

    pub fn as_date(&self) -> Option<NaiveDate> {
        match self {
            Constant::Date(d) => Some(*d),
            _ => None,
        }
    }

    pub fn as_time(&self) -> Option<NaiveTime> {
        match self {
            Constant::Time(t) => Some(*t),
            _ => None,
        }
    }

    pub fn as_datetime(&self) -> Option<DateTime<FixedOffset>> {
        match self {
            Constant::DateTime(dt) => Some(*dt),
            _ => None,
        }
    }

    pub fn as_json(&self) -> Option<&serde_json::Value> {
        match self {
            Constant::Json(j) => Some(j),
            _ => None,
        }
    }
}

impl From<i32> for Constant {
    fn from(i: i32) -> Self {
        Constant::Integer(i)
    }
}
impl From<f64> for Constant {
    fn from(d: f64) -> Self {
        Constant::Double(d)
    }
}
impl From<Vec<u8>> for Constant {
    fn from(b: Vec<u8>) -> Self {
        Constant::Bytes(b)
    }
}
impl From<String> for Constant {
    fn from(s: String) -> Self {
        Constant::String(s)
    }
}
impl From<bool> for Constant {
    fn from(b: bool) -> Self {
        Constant::Bool(b)
    }
}
impl From<NaiveDate> for Constant {
    fn from(d: NaiveDate) -> Self {
        Constant::Date(d)
    }
}
impl From<NaiveTime> for Constant {
    fn from(t: NaiveTime) -> Self {
        Constant::Time(t)
    }
}
impl From<DateTime<FixedOffset>> for Constant {
    fn from(dt: DateTime<FixedOffset>) -> Self {
        Constant::DateTime(dt)
    }
}
impl From<serde_json::Value> for Constant {
    fn from(j: serde_json::Value) -> Self {
        Constant::Json(j)
    }
}

impl std::fmt::Display for Constant {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Constant::Integer(i) => write!(f, "{}", i),
            Constant::Double(d) => write!(f, "{}", d),
            Constant::Bytes(b) => write!(f, "{:?}", b),
            Constant::String(s) => write!(f, "{}", s),
            Constant::Bool(b) => write!(f, "{}", b),
            Constant::Date(d) => write!(f, "{}", d),
            Constant::Time(t) => write!(f, "{}", t),
            Constant::DateTime(dt) => write!(f, "{}", dt),
            Constant::Json(j) => write!(f, "{}", j),
        }
    }
}
