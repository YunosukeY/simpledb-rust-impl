#![allow(dead_code)]

use std::collections::HashMap;

use crate::sql::ColumnType::{Integer, Varchar};
use crate::{sql::ColumnType, util::Result};

#[derive(Clone)]
struct FieldInfo {
    pub(super) column_type: ColumnType,
    pub(super) length: i32,
}

impl FieldInfo {
    pub fn new(column_type: ColumnType, length: i32) -> Self {
        Self {
            column_type,
            length,
        }
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

    pub fn add_field(&mut self, fieldname: &str, column_type: ColumnType, length: i32) {
        self.fields.push(fieldname.to_string());
        self.info
            .insert(fieldname.to_string(), FieldInfo::new(column_type, length));
    }

    pub fn add_int_field(&mut self, fieldname: &str) {
        self.add_field(fieldname, Integer, 0);
    }

    pub fn add_string_field(&mut self, fieldname: &str, length: i32) {
        self.add_field(fieldname, Varchar, length);
    }

    pub fn add(&mut self, fieldname: &str, sch: Schema) -> Result<()> {
        let column_type = sch
            .column_type(fieldname)
            .ok_or(format!("Field not found. fieldname: {}", fieldname))?;
        let length = sch
            .length(fieldname)
            .ok_or(format!("Field not found. fieldname: {}", fieldname))?;
        self.add_field(fieldname, column_type, length);
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

    pub fn column_type(&self, fieldname: &str) -> Option<ColumnType> {
        self.info
            .get(fieldname)
            .map(|info| info.column_type.clone())
    }

    pub fn length(&self, fieldname: &str) -> Option<i32> {
        self.info.get(fieldname).map(|info| info.length)
    }
}
