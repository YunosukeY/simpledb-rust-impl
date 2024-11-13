#![allow(dead_code)]

use std::sync::Arc;

use crate::{
    file::block_id::BlockId,
    query::{constant::Constant, scan::Scan, update_scan::UpdateScan},
    sql::ColumnType,
    tx::transaction::Transaction,
};

use super::{layout::Layout, record_page::RecordPage, rid::Rid};

pub struct TableScan<'a> {
    tx: Arc<Transaction<'a>>,
    layout: Arc<Layout>,
    rp: Option<RecordPage<'a>>,
    filename: String,
    current_slot: i32,
}

impl<'a> TableScan<'a> {
    pub fn new(tx: Arc<Transaction<'a>>, table_name: &str, layout: Arc<Layout>) -> Self {
        let mut scan = Self {
            tx,
            layout,
            rp: None,
            filename: format!("{}.tbl", table_name),
            current_slot: 0,
        };
        let tx = Arc::as_ptr(&scan.tx) as *mut Transaction;
        if unsafe { (*tx).size(&scan.filename).unwrap() == 0 } {
            scan.move_to_new_block();
        } else {
            scan.move_to_block(0);
        }
        scan
    }
}

impl<'a> Scan for TableScan<'a> {
    fn before_first(&mut self) {
        self.move_to_block(0);
    }

    fn next(&mut self) -> bool {
        self.current_slot = self.rp.as_mut().unwrap().next_after(self.current_slot);
        while self.current_slot < 0 {
            if self.at_last_block() {
                return false;
            }
            self.move_to_block(self.rp.as_ref().unwrap().block().block_num() + 1);
            self.current_slot = self.rp.as_mut().unwrap().next_after(self.current_slot);
        }
        true
    }

    fn get_int(&mut self, field_name: &str) -> i32 {
        self.rp
            .as_mut()
            .unwrap()
            .get_int(self.current_slot, field_name)
    }

    fn get_double(&mut self, field_name: &str) -> f64 {
        self.rp
            .as_mut()
            .unwrap()
            .get_double(self.current_slot, field_name)
    }

    fn get_bytes(&mut self, field_name: &str) -> Vec<u8> {
        self.rp
            .as_mut()
            .unwrap()
            .get_bytes(self.current_slot, field_name)
    }

    fn get_string(&mut self, field_name: &str) -> String {
        self.rp
            .as_mut()
            .unwrap()
            .get_string(self.current_slot, field_name)
    }

    fn get_boolean(&mut self, field_name: &str) -> bool {
        self.rp
            .as_mut()
            .unwrap()
            .get_bool(self.current_slot, field_name)
    }

    fn get_date(&mut self, field_name: &str) -> chrono::NaiveDate {
        self.rp
            .as_mut()
            .unwrap()
            .get_date(self.current_slot, field_name)
    }

    fn get_time(&mut self, field_name: &str) -> chrono::NaiveTime {
        self.rp
            .as_mut()
            .unwrap()
            .get_time(self.current_slot, field_name)
    }

    fn get_datetime(&mut self, field_name: &str) -> chrono::DateTime<chrono::FixedOffset> {
        self.rp
            .as_mut()
            .unwrap()
            .get_datetime(self.current_slot, field_name)
    }

    fn get_json(&mut self, field_name: &str) -> serde_json::Value {
        self.rp
            .as_mut()
            .unwrap()
            .get_json(self.current_slot, field_name)
    }

    fn get_value(&mut self, field_name: &str) -> Constant {
        let layout = Arc::as_ptr(&self.layout);
        match unsafe { (*layout).schema().column_type(field_name).unwrap() } {
            ColumnType::Integer => Constant::from(self.get_int(field_name)),
            ColumnType::Double => Constant::from(self.get_double(field_name)),
            ColumnType::VarBit => Constant::from(self.get_bytes(field_name)),
            ColumnType::VarChar => Constant::from(self.get_string(field_name)),
            ColumnType::Boolean => Constant::from(self.get_boolean(field_name)),
            ColumnType::Date => Constant::from(self.get_date(field_name)),
            ColumnType::Time => Constant::from(self.get_time(field_name)),
            ColumnType::DateTime => Constant::from(self.get_datetime(field_name)),
            ColumnType::Json => Constant::from(self.get_json(field_name)),
        }
    }

    fn has_field(&self, field_name: &str) -> bool {
        let layout = Arc::as_ptr(&self.layout);
        unsafe { (*layout).schema().has_field(field_name) }
    }

    fn close(&self) {
        if self.rp.is_some() {
            let tx = Arc::as_ptr(&self.tx) as *mut Transaction;
            unsafe { (*tx).unpin(self.rp.as_ref().unwrap().block()) };
        }
    }
}

impl<'a> UpdateScan for TableScan<'a> {
    fn set_value(&mut self, field_name: &str, value: Constant) {
        let layout = Arc::as_ptr(&self.layout);
        match unsafe { (*layout).schema().column_type(field_name).unwrap() } {
            ColumnType::Integer => self.set_int(field_name, value.as_int().unwrap()),
            ColumnType::Double => self.set_double(field_name, value.as_double().unwrap()),
            ColumnType::VarBit => self.set_bytes(field_name, value.as_bytes().unwrap()),
            ColumnType::VarChar => self.set_string(field_name, value.as_string().unwrap()),
            ColumnType::Boolean => self.set_boolean(field_name, value.as_boolean().unwrap()),
            ColumnType::Date => self.set_date(field_name, value.as_date().unwrap()),
            ColumnType::Time => self.set_time(field_name, value.as_time().unwrap()),
            ColumnType::DateTime => self.set_datetime(field_name, value.as_datetime().unwrap()),
            ColumnType::Json => self.set_json(field_name, value.as_json().unwrap()),
        }
    }

    fn set_int(&mut self, field_name: &str, value: i32) {
        self.rp
            .as_mut()
            .unwrap()
            .set_int(self.current_slot, field_name, value);
    }

    fn set_double(&mut self, field_name: &str, value: f64) {
        self.rp
            .as_mut()
            .unwrap()
            .set_double(self.current_slot, field_name, value);
    }

    fn set_bytes(&mut self, field_name: &str, value: &[u8]) {
        self.rp
            .as_mut()
            .unwrap()
            .set_bytes(self.current_slot, field_name, value);
    }

    fn set_string(&mut self, field_name: &str, value: &str) {
        self.rp
            .as_mut()
            .unwrap()
            .set_string(self.current_slot, field_name, value);
    }

    fn set_boolean(&mut self, field_name: &str, value: bool) {
        self.rp
            .as_mut()
            .unwrap()
            .set_bool(self.current_slot, field_name, value);
    }

    fn set_date(&mut self, field_name: &str, value: chrono::NaiveDate) {
        self.rp
            .as_mut()
            .unwrap()
            .set_date(self.current_slot, field_name, value);
    }

    fn set_time(&mut self, field_name: &str, value: chrono::NaiveTime) {
        self.rp
            .as_mut()
            .unwrap()
            .set_time(self.current_slot, field_name, value);
    }

    fn set_datetime(&mut self, field_name: &str, value: chrono::DateTime<chrono::FixedOffset>) {
        self.rp
            .as_mut()
            .unwrap()
            .set_datetime(self.current_slot, field_name, value);
    }

    fn set_json(&mut self, field_name: &str, value: &serde_json::Value) {
        self.rp
            .as_mut()
            .unwrap()
            .set_json(self.current_slot, field_name, value);
    }

    fn insert(&mut self) {
        self.current_slot = self.rp.as_mut().unwrap().insert_after(self.current_slot);
        while self.current_slot < 0 {
            if self.at_last_block() {
                self.move_to_new_block();
            } else {
                self.move_to_block(self.rp.as_ref().unwrap().block().block_num() + 1);
            }
            self.current_slot = self.rp.as_mut().unwrap().insert_after(self.current_slot);
        }
    }

    fn delete(&mut self) {
        self.rp.as_mut().unwrap().delete(self.current_slot);
    }

    fn get_rid(&self) -> Rid {
        let block_num = self.rp.as_ref().unwrap().block().block_num();
        Rid::new(block_num, self.current_slot)
    }

    fn move_to_rid(&mut self, rid: Rid) {
        self.close();
        let block = BlockId::new(self.filename.clone(), rid.block_num());
        self.rp = Some(RecordPage::new(self.tx.clone(), block, self.layout.clone()));
        self.current_slot = rid.slot();
    }
}

impl<'a> TableScan<'a> {
    fn move_to_block(&mut self, block_num: i32) {
        self.close();
        let block = BlockId::new(self.filename.clone(), block_num);
        self.rp = Some(RecordPage::new(self.tx.clone(), block, self.layout.clone()));
        self.current_slot = -1;
    }

    fn move_to_new_block(&mut self) {
        self.close();
        let tx = Arc::as_ptr(&self.tx) as *mut Transaction;
        let block = unsafe { (*tx).append(&self.filename).unwrap() };
        self.rp = Some(RecordPage::new(self.tx.clone(), block, self.layout.clone()));
        self.current_slot = -1;
    }

    fn at_last_block(&mut self) -> bool {
        let block_num = self.rp.as_ref().unwrap().block().block_num();
        let tx = Arc::as_ptr(&self.tx) as *mut Transaction;
        unsafe { block_num == (*tx).size(&self.filename).unwrap() - 1 }
    }
}
