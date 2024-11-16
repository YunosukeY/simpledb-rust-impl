#![allow(dead_code)]

use std::collections::HashMap;

use crate::{
    file::page::{Page, DATETIME_LEN, DATE_LEN, TIME_LEN},
    sql::ColumnType,
    util::{BOOL_BYTES, DOUBLE_BYTES, INTEGER_BYTES},
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

    pub fn offset(&self, field_name: &str) -> Option<&i32> {
        self.offsets.get(field_name)
    }

    pub fn slot_size(&self) -> i32 {
        self.slot_size
    }

    fn length_in_bytes(&self, field_name: &str) -> Option<i32> {
        let column_type = self.schema.column_type(field_name)?;
        match column_type {
            ColumnType::Integer => Some(INTEGER_BYTES),
            ColumnType::Double => Some(DOUBLE_BYTES),
            ColumnType::VarBit => Some(Page::max_bytes_len(self.schema.length(field_name)?)),
            ColumnType::VarChar => Some(Page::max_str_len(self.schema.length(field_name)?)),
            ColumnType::Boolean => Some(BOOL_BYTES),
            ColumnType::Date => Some(DATE_LEN),
            ColumnType::Time => Some(TIME_LEN),
            ColumnType::DateTime => Some(DATETIME_LEN),
            ColumnType::Json => Some(Page::max_json_len(self.schema.length(field_name)?)),
        }
    }
}

impl From<Schema> for Layout {
    fn from(schema: Schema) -> Self {
        let mut layout = Layout::new(schema.clone(), HashMap::new(), 0);
        let mut pos = BOOL_BYTES;
        for field_name in schema.fields() {
            layout.offsets.insert(field_name.clone(), pos);
            pos += layout.length_in_bytes(field_name).unwrap();
        }
        layout.slot_size = pos;
        layout
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_layout() {
        let mut schema = Schema::new();
        schema
            .add_int_field("int")
            .add_double_field("double")
            .add_bytes_field("bytes", 100)
            .add_string_field("string", 100)
            .add_boolean_field("boolean")
            .add_date_field("date")
            .add_time_field("time")
            .add_datetime_field("datetime")
            .add_json_field("json", 100);

        let layout = Layout::from(schema);

        assert_eq!(*layout.offset("int").unwrap(), 1);
        assert_eq!(*layout.offset("double").unwrap(), 5);
        assert_eq!(*layout.offset("bytes").unwrap(), 13);
        assert_eq!(*layout.offset("string").unwrap(), 117);
        assert_eq!(*layout.offset("boolean").unwrap(), 521);
        assert_eq!(*layout.offset("date").unwrap(), 522);
        assert_eq!(*layout.offset("time").unwrap(), 528);
        assert_eq!(*layout.offset("datetime").unwrap(), 535);
        assert_eq!(*layout.offset("json").unwrap(), 550);
        assert_eq!(layout.slot_size(), 954);
    }
}
