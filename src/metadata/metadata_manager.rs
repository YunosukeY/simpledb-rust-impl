#![allow(dead_code)]

use std::{collections::HashMap, sync::Arc};

use crate::{
    record::{layout::Layout, schema::Schema},
    tx::transaction::Transaction,
};

use super::{
    index_info::IndexInfo, index_manager::IndexManager, stat_info::StatInfo,
    stat_manager::StatManager, table_manager::TableManager, view_manager::ViewManager,
};

#[derive(Clone)]
pub struct MetadataManager {
    tm: Arc<TableManager>,
    vm: ViewManager,
    sm: Arc<StatManager>,
    im: IndexManager,
}

impl MetadataManager {
    pub fn new(is_new: bool, tx: Arc<Transaction>) -> Self {
        let tm = Arc::new(TableManager::new(is_new, tx.clone()));
        let vm = ViewManager::new(is_new, tm.clone(), tx.clone());
        let sm = Arc::new(StatManager::new(tm.clone(), tx.clone()));
        let im = IndexManager::new(is_new, tm.clone(), sm.clone(), tx.clone());
        Self { tm, vm, sm, im }
    }

    pub fn create_table(&self, table_name: &str, schema: Schema, tx: Transaction) {
        let tx = Arc::new(tx);
        self.tm.create_table(table_name, schema, tx);
    }

    pub fn get_layout(&self, table_name: &str, tx: Transaction) -> Layout {
        let tx = Arc::new(tx);
        self.tm.get_layout(table_name, tx)
    }

    pub fn create_view(&self, view_name: &str, view_def: &str, tx: Transaction) {
        self.vm.create_view(view_name, view_def, tx);
    }

    pub fn get_view_def(&self, view_name: &str, tx: Transaction) -> Option<String> {
        self.vm.get_view_def(view_name, tx)
    }

    pub fn create_index(
        &self,
        index_name: &str,
        table_name: &str,
        field_name: &str,
        tx: Transaction,
    ) {
        self.im.create_index(index_name, table_name, field_name, tx);
    }

    pub fn get_index_info<'a>(
        &'a mut self,
        table_name: &str,
        tx: Transaction<'a>,
    ) -> HashMap<String, IndexInfo> {
        self.im.get_index_info(table_name, tx)
    }

    pub fn get_stat_info(&self, table_name: &str, layout: Layout, tx: Transaction) -> StatInfo {
        let tx = Arc::new(tx);
        let sm = Arc::as_ptr(&self.sm) as *mut StatManager;
        unsafe { (*sm).get_stat_info(table_name, layout, tx) }
    }
}
