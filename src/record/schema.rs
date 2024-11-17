#![allow(dead_code)]

use std::collections::HashMap;

use crate::sql::ColumnType;

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

    pub fn add_field(
        &mut self,
        fieldname: &str,
        column_type: ColumnType,
        length: i32,
    ) -> &mut Self {
        if self.fields.len() == 31 {
            return self;
        }
        self.fields.push(fieldname.to_string());
        self.info
            .insert(fieldname.to_string(), FieldInfo::new(column_type, length));
        self
    }

    pub fn add_int_field(&mut self, fieldname: &str) -> &mut Self {
        self.add_field(fieldname, ColumnType::Integer, 0)
    }

    pub fn add_double_field(&mut self, fieldname: &str) -> &mut Self {
        self.add_field(fieldname, ColumnType::Double, 0)
    }

    pub fn add_bytes_field(&mut self, fieldname: &str, length: i32) -> &mut Self {
        self.add_field(fieldname, ColumnType::VarBit, length)
    }

    pub fn add_string_field(&mut self, fieldname: &str, length: i32) -> &mut Self {
        self.add_field(fieldname, ColumnType::VarChar, length)
    }

    pub fn add_boolean_field(&mut self, fieldname: &str) -> &mut Self {
        self.add_field(fieldname, ColumnType::Boolean, 0)
    }

    pub fn add_date_field(&mut self, fieldname: &str) -> &mut Self {
        self.add_field(fieldname, ColumnType::Date, 0)
    }

    pub fn add_time_field(&mut self, fieldname: &str) -> &mut Self {
        self.add_field(fieldname, ColumnType::Time, 0)
    }

    pub fn add_datetime_field(&mut self, fieldname: &str) -> &mut Self {
        self.add_field(fieldname, ColumnType::DateTime, 0)
    }

    pub fn add_json_field(&mut self, fieldname: &str, length: i32) -> &mut Self {
        self.add_field(fieldname, ColumnType::Json, length)
    }

    pub fn add(&mut self, fieldname: &str, sch: Schema) -> &mut Self {
        if !sch.has_field(fieldname) {
            return self;
        }
        let column_type = sch.column_type(fieldname).unwrap();
        let length = sch.length(fieldname).unwrap();
        self.add_field(fieldname, column_type, length)
    }

    pub fn add_all(&mut self, sch: Schema) -> &mut Self {
        for fieldname in sch.fields() {
            self.add(fieldname, sch.clone());
        }
        self
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
