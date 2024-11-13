#![allow(dead_code)]

use std::sync::Arc;

use crate::{
    file::block_id::BlockId,
    sql::ColumnType::{Integer, Varchar},
    tx::transaction::Transaction,
};

use super::layout::Layout;

const EMPTY: bool = false;
const USED: bool = true;

pub struct RecordPage<'a> {
    tx: Arc<Transaction<'a>>,
    block: BlockId,
    layout: Arc<Layout>,
}

impl<'a> RecordPage<'a> {
    pub fn new(tx: Arc<Transaction<'a>>, block: BlockId, layout: Arc<Layout>) -> Self {
        Self { tx, block, layout }
    }

    pub fn get_int(&mut self, slot: i32, field_name: &str) -> i32 {
        let field_pos = self.field_pos(slot, field_name);
        let tx = Arc::as_ptr(&self.tx) as *mut Transaction;
        unsafe { (*tx).get_int(&self.block, field_pos).unwrap() }
    }

    pub fn set_int(&mut self, slot: i32, field_name: &str, value: i32) {
        let field_pos = self.field_pos(slot, field_name);
        let tx = Arc::as_ptr(&self.tx) as *mut Transaction;
        unsafe {
            (*tx).set_int(&self.block, field_pos, value, true).unwrap();
        }
    }

    pub fn get_string(&mut self, slot: i32, field_name: &str) -> String {
        let field_pos = self.field_pos(slot, field_name);
        let tx = Arc::as_ptr(&self.tx) as *mut Transaction;
        unsafe { (*tx).get_string(&self.block, field_pos).unwrap() }
    }

    pub fn set_string(&mut self, slot: i32, field_name: &str, value: &str) {
        let field_pos = self.field_pos(slot, field_name);
        let tx = Arc::as_ptr(&self.tx) as *mut Transaction;
        unsafe {
            (*tx)
                .set_string(&self.block, field_pos, value, true)
                .unwrap();
        }
    }

    pub fn delete(&mut self, slot: i32) {
        self.set_flag(slot, EMPTY);
    }

    pub fn format(&mut self) {
        let mut slot = 0;
        while self.is_valid_slot(slot) {
            let tx = Arc::as_ptr(&self.tx) as *mut Transaction;
            unsafe {
                (*tx)
                    .set_bool(&self.block, self.offset(slot), EMPTY, false)
                    .unwrap();
            }
            let schema = self.layout.schema();
            for field_name in schema.fields() {
                let field_pos = self.field_pos(slot, field_name);
                let column_type = schema.column_type(field_name).unwrap();
                match column_type {
                    Integer => unsafe { (*tx).set_int(&self.block, field_pos, 0, false).unwrap() },
                    Varchar => unsafe {
                        (*tx).set_string(&self.block, field_pos, "", false).unwrap()
                    },
                };
                slot += 1;
            }
        }
    }

    pub fn next_after(&mut self, slot: i32) -> i32 {
        self.search_after(slot, USED)
    }

    pub fn insert_after(&mut self, slot: i32) -> i32 {
        let new_slot = self.search_after(slot, EMPTY);
        if new_slot >= 0 {
            self.set_flag(new_slot, USED);
        }
        new_slot
    }

    pub fn block(&self) -> &BlockId {
        &self.block
    }

    fn field_pos(&self, slot: i32, field_name: &str) -> i32 {
        self.offset(slot) + self.layout.offset(field_name)
    }

    fn set_flag(&mut self, slot: i32, flag: bool) {
        let tx = Arc::as_ptr(&self.tx) as *mut Transaction;
        unsafe {
            (*tx)
                .set_bool(&self.block, self.offset(slot), flag, true)
                .unwrap();
        }
    }

    fn search_after(&mut self, slot: i32, flag: bool) -> i32 {
        let mut next_slot = slot + 1;
        while self.is_valid_slot(next_slot) {
            let tx = Arc::as_ptr(&self.tx) as *mut Transaction;
            if unsafe { (*tx).get_bool(&self.block, self.offset(next_slot)).unwrap() == flag } {
                return next_slot;
            }
            next_slot += 1;
        }
        -1
    }

    fn is_valid_slot(&self, slot: i32) -> bool {
        self.offset(slot + 1) <= self.tx.block_size()
    }

    fn offset(&self, slot: i32) -> i32 {
        self.layout.slot_size() * slot
    }
}
