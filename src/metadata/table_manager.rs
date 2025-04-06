#![allow(dead_code)]

use std::{collections::HashMap, sync::Arc};

use crate::{
    query::{scan::Scan, update_scan::UpdateScan},
    record::{
        layout::{Layout, IS_USED_FLAG_NAME},
        schema::Schema,
        table_scan::TableScan,
    },
    sql::ColumnType,
    tx::transaction::Transaction,
};

pub(super) const MAX_NAME_LENGTH: i32 = 16;

// table names
pub(super) const TABLE_CATALOG: &str = "tblcat";
const FIELD_CATALOG: &str = "fldcat";

// filed names
pub(super) const TABLE_NAME: &str = "tblname";
const SLOT_SIZE: &str = "slotsize";
const FIELD_NAME: &str = "fldname";
const TYPE: &str = "type";
const LENGTH: &str = "length";
const OFFSET: &str = "offset";
const BIT_FLAG: &str = "bitflag";

pub(super) struct TableManager {
    tcat_layout: Layout,
    fcat_layout: Layout,
}

impl TableManager {
    pub fn new(is_new: bool, tx: Arc<Transaction>) -> Self {
        let mut tcat_schema = Schema::new();
        tcat_schema
            .add_string_field(TABLE_NAME, MAX_NAME_LENGTH)
            .add_int_field(SLOT_SIZE);
        let tcat_layout = Layout::from(tcat_schema);

        let mut fcat_schema = Schema::new();
        fcat_schema
            .add_string_field(TABLE_NAME, MAX_NAME_LENGTH)
            .add_string_field(FIELD_NAME, MAX_NAME_LENGTH)
            .add_int_field(TYPE)
            .add_int_field(LENGTH)
            .add_int_field(OFFSET)
            .add_int_field(BIT_FLAG);
        let fcat_layout = Layout::from(fcat_schema);

        let tm = Self {
            tcat_layout,
            fcat_layout,
        };

        if is_new {
            tm.create_table(TABLE_CATALOG, tm.tcat_layout.schema().clone(), tx.clone());
            tm.create_table(FIELD_CATALOG, tm.fcat_layout.schema().clone(), tx.clone());
        }

        tm
    }

    pub fn create_table(&self, table_name: &str, schema: Schema, tx: Arc<Transaction>) {
        let layout = Layout::from(schema);

        let mut tcat = TableScan::new(tx.clone(), TABLE_CATALOG, self.tcat_layout.clone()).unwrap();
        tcat.insert().unwrap();
        tcat.set_string(TABLE_NAME, table_name)
            .set_int(SLOT_SIZE, layout.slot_size());
        tcat.close();

        let mut fcat = TableScan::new(tx.clone(), FIELD_CATALOG, self.fcat_layout.clone()).unwrap();
        for field_name in layout.schema().fields() {
            fcat.insert().unwrap();
            fcat.set_string(TABLE_NAME, table_name)
                .set_string(FIELD_NAME, field_name)
                .set_int(
                    TYPE,
                    layout.schema().column_type(field_name).unwrap() as i32,
                )
                .set_int(LENGTH, layout.schema().length(field_name).unwrap())
                .set_int(OFFSET, *layout.offset(field_name).unwrap())
                .set_int(BIT_FLAG, *layout.flag_bit_location(field_name).unwrap());
        }
        fcat.close();
    }

    pub fn get_layout(&self, table_name: &str, tx: Arc<Transaction>) -> Layout {
        let mut size = -1;
        let mut tcat = TableScan::new(tx.clone(), TABLE_CATALOG, self.tcat_layout.clone()).unwrap();
        while tcat.next().unwrap() {
            if tcat.get_string(TABLE_NAME).unwrap() == table_name {
                size = tcat.get_int(SLOT_SIZE).unwrap();
                break;
            }
        }
        tcat.close();

        let mut schema = Schema::new();
        let mut offsets = HashMap::new();
        let mut flag_bit_location = HashMap::new();
        flag_bit_location.insert(IS_USED_FLAG_NAME.to_string(), 0);
        let mut fcat = TableScan::new(tx.clone(), FIELD_CATALOG, self.fcat_layout.clone()).unwrap();
        while fcat.next().unwrap() {
            if fcat.get_string(TABLE_NAME).unwrap() == table_name {
                let field_name = fcat.get_string(FIELD_NAME).unwrap();
                let column_type = fcat.get_int(TYPE).unwrap();
                let length = fcat.get_int(LENGTH).unwrap();
                let offset = fcat.get_int(OFFSET).unwrap();
                let bit_flag = fcat.get_int(BIT_FLAG).unwrap();

                schema.add_field(
                    &field_name,
                    ColumnType::try_from(column_type).unwrap(),
                    length,
                );
                offsets.insert(field_name.clone(), offset);
                flag_bit_location.insert(field_name.clone(), bit_flag);
            }
        }
        fcat.close();
        Layout::new(schema, offsets, flag_bit_location, size)
    }
}
