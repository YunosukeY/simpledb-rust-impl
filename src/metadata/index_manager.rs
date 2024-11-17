#![allow(dead_code)]

use std::{collections::HashMap, sync::Arc};

use crate::{
    query::{scan::Scan, update_scan::UpdateScan},
    record::{layout::Layout, schema::Schema, table_scan::TableScan},
    tx::transaction::Transaction,
};

use super::{
    index_info::IndexInfo,
    stat_manager::StatManager,
    table_manager::{TableManager, MAX_NAME_LENGTH},
};

// table names
const INDEX_CATALOG: &str = "idxcat";

// field names
const INDEX_NAME: &str = "idxname";
const TABLE_NAME: &str = "tblname";
const FIELD_NAME: &str = "fldname";

#[derive(Clone)]
pub(super) struct IndexManager {
    layout: Layout,
    tm: Arc<TableManager>,
    sm: Arc<StatManager>,
}

impl IndexManager {
    pub fn new(
        is_new: bool,
        tm: Arc<TableManager>,
        sm: Arc<StatManager>,
        tx: Arc<Transaction>,
    ) -> Self {
        if is_new {
            let mut schema = Schema::new();
            schema
                .add_string_field(INDEX_NAME, MAX_NAME_LENGTH)
                .add_string_field(TABLE_NAME, MAX_NAME_LENGTH)
                .add_string_field(FIELD_NAME, MAX_NAME_LENGTH);
            tm.create_table(INDEX_CATALOG, schema, tx.clone());
        }
        let layout = tm.get_layout(INDEX_CATALOG, tx.clone());
        Self { layout, tm, sm }
    }

    pub fn create_index(
        &self,
        index_name: &str,
        table_name: &str,
        field_name: &str,
        tx: Transaction,
    ) {
        let tx = Arc::new(tx);
        let mut ts = TableScan::new(tx, INDEX_CATALOG, self.layout.clone()).unwrap();
        ts.insert().unwrap();
        ts.set_string(INDEX_NAME, index_name)
            .set_string(TABLE_NAME, table_name)
            .set_string(FIELD_NAME, field_name);
        ts.close();
    }

    pub fn get_index_info<'a>(
        &'a mut self,
        table_name: &str,
        tx: Transaction<'a>,
    ) -> HashMap<String, IndexInfo> {
        let tx = Arc::new(tx);
        let mut result = HashMap::new();
        let mut ts = TableScan::new(tx.clone(), INDEX_CATALOG, self.layout.clone()).unwrap();
        while ts.next().unwrap() {
            if ts.get_string(TABLE_NAME).unwrap() == table_name {
                let index_name = ts.get_string(INDEX_NAME).unwrap();
                let field_name = ts.get_string(FIELD_NAME).unwrap();
                let table_layout = self.tm.get_layout(table_name, tx.clone());
                let sm = Arc::as_ptr(&self.sm) as *mut StatManager;
                let table_si =
                    unsafe { (*sm).get_stat_info(table_name, table_layout.clone(), tx.clone()) };
                let ii = IndexInfo::new(
                    index_name.clone(),
                    field_name,
                    table_layout.schema().clone(),
                    tx.clone(),
                    table_si,
                );
                result.insert(index_name, ii);
            }
        }
        ts.close();
        result
    }
}
