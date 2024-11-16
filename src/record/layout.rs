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

    pub fn offset(&self, field_name: &str) -> &i32 {
        self.offsets.get(field_name).unwrap()
    }

    pub fn slot_size(&self) -> i32 {
        self.slot_size
    }

    fn length_in_bytes(&self, field_name: &str) -> i32 {
        let column_type = self.schema.column_type(field_name).unwrap();
        match column_type {
            ColumnType::Integer => INTEGER_BYTES,
            ColumnType::Double => DOUBLE_BYTES,
            ColumnType::VarBit => Page::max_bytes_len(self.schema.length(field_name).unwrap()),
            ColumnType::VarChar => Page::max_str_len(self.schema.length(field_name).unwrap()),
            ColumnType::Boolean => BOOL_BYTES,
            ColumnType::Date => DATE_LEN,
            ColumnType::Time => TIME_LEN,
            ColumnType::DateTime => DATETIME_LEN,
            ColumnType::Json => Page::max_json_len(self.schema.length(field_name).unwrap()),
        }
    }
}

impl From<Schema> for Layout {
    fn from(schema: Schema) -> Self {
        let mut layout = Layout::new(schema.clone(), HashMap::new(), 0);
        let mut pos = BOOL_BYTES;
        for field_name in schema.fields() {
            layout.offsets.insert(field_name.clone(), pos);
            pos += layout.length_in_bytes(field_name)
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

        assert_eq!(*layout.offset("int"), 1);
        assert_eq!(*layout.offset("double"), 5);
        assert_eq!(*layout.offset("bytes"), 13);
        assert_eq!(*layout.offset("string"), 117);
        assert_eq!(*layout.offset("boolean"), 521);
        assert_eq!(*layout.offset("date"), 522);
        assert_eq!(*layout.offset("time"), 528);
        assert_eq!(*layout.offset("datetime"), 535);
        assert_eq!(*layout.offset("json"), 550);
        assert_eq!(layout.slot_size(), 954);
    }
}
