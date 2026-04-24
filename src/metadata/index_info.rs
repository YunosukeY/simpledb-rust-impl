#![allow(dead_code)]

use std::sync::Arc;

use crate::{
    index::{
        hash::hash_index::HashIndex,
        index::{Index, BLOCK, ID},
    },
    record::{layout::Layout, schema::Schema},
    sql::ColumnType,
    tx::transaction::Transaction,
};

use super::stat_info::StatInfo;

pub struct IndexInfo<'a> {
    index_name: String,
    field_name: String,
    tx: Arc<Transaction<'a>>,
    table_schema: Schema,
    index_layout: Layout,
    si: StatInfo,
}

impl<'a> IndexInfo<'a> {
    pub fn new(
        index_name: String,
        field_name: String,
        table_schema: Schema,
        tx: Arc<Transaction<'a>>,
        si: StatInfo,
    ) -> Self {
        let index_layout = Self::create_index_layout(&table_schema, &field_name);
        Self {
            index_name,
            field_name,
            tx,
            table_schema,
            index_layout,
            si,
        }
    }

    pub fn open(&self) -> Box<dyn Index> {
        Box::new(HashIndex::new())
        // Box::new(BTreeIndex::new())
    }

    pub fn blocks_accessed(&self) -> i32 {
        let rpb = self.tx.block_size() / self.index_layout.slot_size();
        let num_blocks = self.si.records_output() / rpb;
        HashIndex::search_cost(num_blocks, rpb)
    }

    pub fn records_output(&self) -> i32 {
        self.si.records_output() / self.si.distincs_values(&self.field_name)
    }

    pub fn distincs_values(&self, field_name: &str) -> i32 {
        if self.field_name == field_name {
            1
        } else {
            self.si.distincs_values(field_name)
        }
    }

    fn create_index_layout(table_schema: &Schema, field_name: &str) -> Layout {
        let mut schema = Schema::new();
        schema.add_int_field(BLOCK).add_int_field(ID);
        let length = table_schema.length(field_name).unwrap();
        match table_schema.column_type(field_name).unwrap() {
            ColumnType::Integer => schema.add_int_field(field_name),
            ColumnType::Double => schema.add_double_field(field_name),
            ColumnType::VarBit => schema.add_bytes_field(field_name, length),
            ColumnType::VarChar => schema.add_string_field(field_name, length),
            ColumnType::Boolean => schema.add_boolean_field(field_name),
            ColumnType::Date => schema.add_date_field(field_name),
            ColumnType::Time => schema.add_time_field(field_name),
            ColumnType::DateTime => schema.add_datetime_field(field_name),
            ColumnType::Json => schema.add_json_field(field_name, length),
        };
        Layout::from(schema)
    }
}
