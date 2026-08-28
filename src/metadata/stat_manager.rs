#![allow(dead_code)]

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use crate::{
    query::{scan::Scan, update_scan::UpdateScan},
    record::{layout::Layout, table_scan::TableScan},
    tx::transaction::Transaction,
};

use super::{
    stat_info::StatInfo,
    table_manager::{TableManager, TABLE_CATALOG, TABLE_NAME},
};

pub(super) struct StatManager {
    m: Mutex<()>,
    tm: Arc<TableManager>,
    table_stats: HashMap<String, StatInfo>,
    num_calls: i32,
}

impl StatManager {
    pub fn new(tm: Arc<TableManager>, tx: Arc<Transaction>) -> Self {
        let table_stats = StatManager::reflesh_stat(&tm, tx);
        Self {
            m: Mutex::new(()),
            tm,
            table_stats,
            num_calls: 0,
        }
    }

    pub fn get_stat_info(
        &mut self,
        table_name: &str,
        layout: Layout,
        tx: Arc<Transaction>,
    ) -> StatInfo {
        let _lock = self.m.lock().unwrap();

        self.num_calls += 1;
        if self.num_calls > 100 {
            self.num_calls = 0;
            self.table_stats = StatManager::reflesh_stat(&self.tm, tx.clone());
        }
        if !self.table_stats.contains_key(table_name) {
            let stat_info = Self::calc_table_stats(table_name, layout, tx.clone());
            self.table_stats.insert(table_name.to_string(), stat_info);
        }
        self.table_stats.get(table_name).unwrap().clone()
    }

    fn reflesh_stat(tm: &TableManager, tx: Arc<Transaction>) -> HashMap<String, StatInfo> {
        let mut table_stats = HashMap::new();
        let tcat_layout = tm.get_layout(TABLE_CATALOG, tx.clone());
        let mut tcat = TableScan::new(tx.clone(), TABLE_CATALOG, tcat_layout).unwrap();
        while tcat.next().unwrap() {
            let table_name = tcat.get_string(TABLE_NAME).unwrap();
            let layout = tm.get_layout(&table_name, tx.clone());
            let stat_info = Self::calc_table_stats(&table_name, layout, tx.clone());
            table_stats.insert(table_name, stat_info);
        }
        tcat.close();
        table_stats
    }

    fn calc_table_stats(table_name: &str, layout: Layout, tx: Arc<Transaction>) -> StatInfo {
        let mut num_records = 0;
        let mut num_blocks = 0;
        let mut ts = TableScan::new(tx, table_name, layout).unwrap();
        while ts.next().unwrap() {
            num_records += 1;
            num_blocks = ts.get_rid().block_num() + 1; // bug?
        }
        ts.close();
        StatInfo::new(num_blocks, num_records)
    }
}
