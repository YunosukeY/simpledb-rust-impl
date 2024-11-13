#![allow(dead_code)]

use std::collections::HashMap;

use crate::{
    file::page::Page,
    sql_types::{INTEGER, VARCHAR},
    util::INTEGER_BYTES,
};

use super::schema::Schema;

pub struct Layout {
    schema: Schema,
    offsets: HashMap<String, i32>,
    slot_size: i32,
}

impl Layout {
    pub fn new(schema: Schema, offsets: HashMap<String, i32>, slot_size: i32) -> Self {
        Self {
            schema,
            offsets,
            slot_size,
        }
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    pub fn offset(&self, field_name: &str) -> &i32 {
        self.offsets.get(field_name).unwrap()
    }

    pub fn slot_size(&self) -> i32 {
        self.slot_size
    }

    fn length_in_bytes(&self, field_name: &str) -> i32 {
        let r#type = self.schema.type_of(field_name).unwrap();
        match r#type {
            INTEGER => INTEGER_BYTES,
            VARCHAR => Page::max_len(self.schema.length(field_name).unwrap()),
            _ => panic!("Unknown type"),
        }
    }
}

impl From<Schema> for Layout {
    fn from(schema: Schema) -> Self {
        let mut layout = Layout::new(schema.clone(), HashMap::new(), 0);
        let mut pos = 0;
        for field_name in schema.fields() {
            layout.offsets.insert(field_name.clone(), pos);
            pos += layout.length_in_bytes(field_name)
        }
        layout.slot_size = pos;
        layout
    }
}
