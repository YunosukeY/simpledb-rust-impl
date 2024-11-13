#![allow(dead_code)]

use std::collections::HashMap;

use crate::{
    sql_types::{INTEGER, VARCHAR},
    util::Result,
};

#[derive(Clone)]
struct FieldInfo {
    pub(super) r#type: i32,
    pub(super) length: i32,
}

impl FieldInfo {
    pub fn new(r#type: i32, length: i32) -> Self {
        Self { r#type, length }
    }
}

#[derive(Clone)]
pub struct Schema {
    fields: Vec<String>,
    info: HashMap<String, FieldInfo>,
}

impl Schema {
    pub fn new() -> Self {
        Self {
            fields: Vec::new(),
            info: HashMap::new(),
        }
    }

    pub fn add_field(&mut self, fieldname: &str, r#type: i32, length: i32) {
        self.fields.push(fieldname.to_string());
        self.info
            .insert(fieldname.to_string(), FieldInfo::new(r#type, length));
    }

    pub fn add_int_field(&mut self, fieldname: &str) {
        self.add_field(fieldname, INTEGER, 0);
    }

    pub fn add_string_field(&mut self, fieldname: &str, length: i32) {
        self.add_field(fieldname, VARCHAR, length);
    }

    pub fn add(&mut self, fieldname: &str, sch: Schema) -> Result<()> {
        let r#type = sch
            .type_of(fieldname)
            .ok_or(format!("Field not found. fieldname: {}", fieldname))?;
        let length = sch
            .length(fieldname)
            .ok_or(format!("Field not found. fieldname: {}", fieldname))?;
        self.add_field(fieldname, r#type, length);
        Ok(())
    }

    pub fn add_all(&mut self, sch: Schema) -> Result<()> {
        for fieldname in sch.fields() {
            self.add(fieldname, sch.clone())?;
        }
        Ok(())
    }

    pub fn fields(&self) -> &Vec<String> {
        &self.fields
    }

    pub fn has_field(&self, fieldname: &str) -> bool {
        self.info.contains_key(fieldname)
    }

    pub fn type_of(&self, fieldname: &str) -> Option<i32> {
        self.info.get(fieldname).map(|info| info.r#type)
    }

    pub fn length(&self, fieldname: &str) -> Option<i32> {
        self.info.get(fieldname).map(|info| info.length)
    }
}
