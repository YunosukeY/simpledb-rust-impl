#![allow(dead_code)]

use std::sync::Arc;

use crate::{
    query::{scan::Scan, update_scan::UpdateScan},
    record::{schema::Schema, table_scan::TableScan},
    tx::transaction::Transaction,
};

use super::table_manager::{TableManager, MAX_NAME_LENGTH};

const MAX_VIEW_DEF_LENGTH: i32 = 100;

// table names
const VIEW_CATALOG: &str = "viewcat";

// field names
const VIEW_NAME: &str = "viewname";
const VIEW_DEF: &str = "viewdef";

#[derive(Clone)]
pub(super) struct ViewManager {
    tm: Arc<TableManager>,
}

impl ViewManager {
    pub fn new(is_new: bool, tm: Arc<TableManager>, tx: Arc<Transaction>) -> Self {
        if is_new {
            let mut schema = Schema::new();
            schema
                .add_string_field(VIEW_NAME, MAX_NAME_LENGTH)
                .add_string_field(VIEW_DEF, MAX_VIEW_DEF_LENGTH);
            tm.create_table(VIEW_CATALOG, schema, tx);
        }

        Self { tm }
    }

    pub fn create_view(&self, view_name: &str, view_def: &str, tx: Transaction) {
        let tx = Arc::new(tx);

        let layout = self.tm.get_layout(VIEW_CATALOG, tx.clone());
        let mut ts = TableScan::new(tx.clone(), VIEW_CATALOG, layout).unwrap();
        ts.insert().unwrap();
        ts.set_string(VIEW_NAME, view_name)
            .set_string(VIEW_DEF, view_def);
        ts.close();
    }

    pub fn get_view_def(&self, view_name: &str, tx: Transaction) -> Option<String> {
        let tx = Arc::new(tx);

        let mut view_def = None;
        let layout = self.tm.get_layout(VIEW_CATALOG, tx.clone());
        let mut ts = TableScan::new(tx.clone(), VIEW_CATALOG, layout).unwrap();
        while ts.next().unwrap() {
            if ts.get_string(VIEW_NAME).unwrap() == view_name {
                view_def = Some(ts.get_string(VIEW_DEF).unwrap());
                break;
            }
        }
        ts.close();
        view_def
    }
}
