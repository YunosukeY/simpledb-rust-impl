#![allow(dead_code)]

use std::sync::Arc;

use crate::{
    file::{block_id::BlockId, page::Page},
    sql::ColumnType,
    tx::transaction::Transaction,
    util::Result,
};

use super::layout::{Layout, IS_USED_FLAG_NAME};

const EMPTY: bool = false;
const USED: bool = true;

pub struct RecordPage<'a> {
    tx: Arc<Transaction<'a>>,
    block: BlockId,
    layout: Arc<Layout>,
}

impl<'a> RecordPage<'a> {
    pub fn new(tx: Arc<Transaction<'a>>, block: BlockId, layout: Arc<Layout>) -> Result<Self> {
        let record = Self { tx, block, layout };
        let tx = Arc::as_ptr(&record.tx) as *mut Transaction;
        unsafe {
            (*tx).pin(&record.block)?;
        }
        Ok(record)
    }

    pub fn get_int(&mut self, slot: i32, field_name: &str) -> Result<i32> {
        let field_pos = self.field_pos(slot, field_name).ok_or("field not found")?;
        let tx = Arc::as_ptr(&self.tx) as *mut Transaction;
        unsafe { (*tx).get_int(&self.block, field_pos) }
    }
    pub fn set_int(&mut self, slot: i32, field_name: &str, value: i32) -> Result<()> {
        let field_pos = self.field_pos(slot, field_name).ok_or("field not found")?;
        let tx = Arc::as_ptr(&self.tx) as *mut Transaction;
        unsafe {
            (*tx).set_int(&self.block, field_pos, value, true)?;
        }
        self._set_null(slot, field_name, false)?;
        Ok(())
    }

    pub fn get_double(&mut self, slot: i32, field_name: &str) -> Result<f64> {
        let field_pos = self.field_pos(slot, field_name).ok_or("field not found")?;
        let tx = Arc::as_ptr(&self.tx) as *mut Transaction;
        unsafe { (*tx).get_double(&self.block, field_pos) }
    }
    pub fn set_double(&mut self, slot: i32, field_name: &str, value: f64) -> Result<()> {
        let field_pos = self.field_pos(slot, field_name).ok_or("field not found")?;
        let tx = Arc::as_ptr(&self.tx) as *mut Transaction;
        unsafe {
            (*tx).set_double(&self.block, field_pos, value, true)?;
        }
        self._set_null(slot, field_name, false)?;
        Ok(())
    }

    pub fn get_bytes(&mut self, slot: i32, field_name: &str) -> Result<Vec<u8>> {
        let field_pos = self.field_pos(slot, field_name).ok_or("field not found")?;
        let tx = Arc::as_ptr(&self.tx) as *mut Transaction;
        unsafe { (*tx).get_bytes(&self.block, field_pos) }
    }
    pub fn set_bytes(&mut self, slot: i32, field_name: &str, value: &[u8]) -> Result<()> {
        let field_pos = self.field_pos(slot, field_name).ok_or("field not found")?;

        if Page::bytes_len(value) > self.layout.length_in_bytes(field_name).unwrap() {
            return Err("bytes too long".into());
        }

        let tx = Arc::as_ptr(&self.tx) as *mut Transaction;
        unsafe {
            (*tx).set_bytes(&self.block, field_pos, value, true)?;
        }
        self._set_null(slot, field_name, false)?;
        Ok(())
    }

    pub fn get_string(&mut self, slot: i32, field_name: &str) -> Result<String> {
        let field_pos = self.field_pos(slot, field_name).ok_or("field not found")?;
        let tx = Arc::as_ptr(&self.tx) as *mut Transaction;
        unsafe { (*tx).get_string(&self.block, field_pos) }
    }
    pub fn set_string(&mut self, slot: i32, field_name: &str, value: &str) -> Result<()> {
        let field_pos = self.field_pos(slot, field_name).ok_or("field not found")?;

        if Page::str_len(value) > self.layout.length_in_bytes(field_name).unwrap() {
            return Err("string too long".into());
        }

        let tx = Arc::as_ptr(&self.tx) as *mut Transaction;
        unsafe {
            (*tx).set_string(&self.block, field_pos, value, true)?;
        }
        self._set_null(slot, field_name, false)?;
        Ok(())
    }

    pub fn get_bool(&mut self, slot: i32, field_name: &str) -> Result<bool> {
        let field_pos = self.field_pos(slot, field_name).ok_or("field not found")?;
        let tx = Arc::as_ptr(&self.tx) as *mut Transaction;
        unsafe { (*tx).get_bool(&self.block, field_pos) }
    }
    pub fn set_bool(&mut self, slot: i32, field_name: &str, value: bool) -> Result<()> {
        let field_pos = self.field_pos(slot, field_name).ok_or("field not found")?;
        let tx = Arc::as_ptr(&self.tx) as *mut Transaction;
        unsafe {
            (*tx).set_bool(&self.block, field_pos, value, true)?;
        }
        self._set_null(slot, field_name, false)?;
        Ok(())
    }

    pub fn get_date(&mut self, slot: i32, field_name: &str) -> Result<chrono::NaiveDate> {
        let field_pos = self.field_pos(slot, field_name).ok_or("field not found")?;
        let tx = Arc::as_ptr(&self.tx) as *mut Transaction;
        let date = unsafe { (*tx).get_date(&self.block, field_pos) };
        date.and_then(|d| d.ok_or("invalid date".into()))
    }
    pub fn set_date(
        &mut self,
        slot: i32,
        field_name: &str,
        value: chrono::NaiveDate,
    ) -> Result<()> {
        let field_pos = self.field_pos(slot, field_name).ok_or("field not found")?;
        let tx = Arc::as_ptr(&self.tx) as *mut Transaction;
        unsafe {
            (*tx).set_date(&self.block, field_pos, &Some(value), true)?;
        }
        self._set_null(slot, field_name, false)?;
        Ok(())
    }

    pub fn get_time(&mut self, slot: i32, field_name: &str) -> Result<chrono::NaiveTime> {
        let field_pos = self.field_pos(slot, field_name).ok_or("field not found")?;
        let tx = Arc::as_ptr(&self.tx) as *mut Transaction;
        let time = unsafe { (*tx).get_time(&self.block, field_pos) };
        time.and_then(|t| t.ok_or("invalid time".into()))
    }
    pub fn set_time(
        &mut self,
        slot: i32,
        field_name: &str,
        value: chrono::NaiveTime,
    ) -> Result<()> {
        let field_pos = self.field_pos(slot, field_name).ok_or("field not found")?;
        let tx = Arc::as_ptr(&self.tx) as *mut Transaction;
        unsafe {
            (*tx).set_time(&self.block, field_pos, &Some(value), true)?;
        }
        self._set_null(slot, field_name, false)?;
        Ok(())
    }

    pub fn get_datetime(
        &mut self,
        slot: i32,
        field_name: &str,
    ) -> Result<chrono::DateTime<chrono::FixedOffset>> {
        let field_pos = self.field_pos(slot, field_name).ok_or("field not found")?;
        let tx = Arc::as_ptr(&self.tx) as *mut Transaction;
        let datetime = unsafe { (*tx).get_datetime(&self.block, field_pos) };
        datetime.and_then(|dt| dt.ok_or("invalid datetime".into()))
    }
    pub fn set_datetime(
        &mut self,
        slot: i32,
        field_name: &str,
        value: chrono::DateTime<chrono::FixedOffset>,
    ) -> Result<()> {
        let field_pos = self.field_pos(slot, field_name).ok_or("field not found")?;
        let tx = Arc::as_ptr(&self.tx) as *mut Transaction;
        unsafe {
            (*tx).set_datetime(&self.block, field_pos, &Some(value), true)?;
        }
        self._set_null(slot, field_name, false)?;
        Ok(())
    }

    pub fn get_json(&mut self, slot: i32, field_name: &str) -> Result<serde_json::Value> {
        let field_pos = self.field_pos(slot, field_name).ok_or("field not found")?;
        let tx = Arc::as_ptr(&self.tx) as *mut Transaction;
        let json = unsafe { (*tx).get_json(&self.block, field_pos) };
        json.and_then(|j| j.ok_or("invalid json".into()))
    }
    pub fn set_json(
        &mut self,
        slot: i32,
        field_name: &str,
        value: &serde_json::Value,
    ) -> Result<()> {
        let field_pos = self.field_pos(slot, field_name).ok_or("field not found")?;

        if Page::json_len(&Some(value.clone())) > self.layout.length_in_bytes(field_name).unwrap() {
            return Err("json too long".into());
        }

        let tx = Arc::as_ptr(&self.tx) as *mut Transaction;
        unsafe {
            (*tx).set_json(&self.block, field_pos, &Some(value.clone()), true)?;
        }
        self._set_null(slot, field_name, false)?;
        Ok(())
    }

    pub fn is_null(&mut self, slot: i32, field_name: &str) -> Result<bool> {
        self.get_flag(slot, field_name)
    }
    pub fn set_null(&mut self, slot: i32, field_name: &str) -> Result<()> {
        self._set_null(slot, field_name, true)
    }
    fn _set_null(&mut self, slot: i32, field_name: &str, null: bool) -> Result<()> {
        self.set_flag(slot, field_name, null)
    }

    pub fn delete(&mut self, slot: i32) -> Result<()> {
        self.set_is_used_flag(slot, EMPTY)
    }

    pub fn format(&mut self) -> Result<()> {
        let mut slot = 0;
        while self.is_valid_slot(slot) {
            let tx = Arc::as_ptr(&self.tx) as *mut Transaction;
            unsafe {
                (*tx).set_int(&self.block, self.offset(slot), 0, false)?;
            }
            let schema = self.layout.schema();
            for field_name in schema.fields() {
                let field_pos = self.field_pos(slot, field_name).unwrap();
                let column_type = schema.column_type(field_name).unwrap();
                match column_type {
                    ColumnType::Integer => unsafe {
                        (*tx).set_int(&self.block, field_pos, 0, false)?;
                    },
                    ColumnType::Double => unsafe {
                        (*tx).set_double(&self.block, field_pos, 0.0, false)?;
                    },
                    ColumnType::VarBit => unsafe {
                        (*tx).set_bytes(&self.block, field_pos, &[], false)?;
                    },
                    ColumnType::VarChar => unsafe {
                        (*tx).set_string(&self.block, field_pos, "", false)?;
                    },
                    ColumnType::Boolean => unsafe {
                        (*tx).set_bool(&self.block, field_pos, false, false)?;
                    },
                    ColumnType::Date => unsafe {
                        (*tx).set_date(&self.block, field_pos, &None, false)?;
                    },
                    ColumnType::Time => unsafe {
                        (*tx).set_time(&self.block, field_pos, &None, false)?;
                    },
                    ColumnType::DateTime => unsafe {
                        (*tx).set_datetime(&self.block, field_pos, &None, false)?;
                    },
                    ColumnType::Json => unsafe {
                        (*tx).set_json(&self.block, field_pos, &None, false)?;
                    },
                };
                slot += 1;
            }
        }
        Ok(())
    }

    pub fn next_after(&mut self, slot: i32) -> Result<i32> {
        self.search_after(slot, USED)
    }

    pub fn insert_after(&mut self, slot: i32) -> Result<i32> {
        let new_slot = self.search_after(slot, EMPTY)?;
        if new_slot >= 0 {
            self.set_is_used_flag(new_slot, USED)?;
        }
        Ok(new_slot)
    }

    pub fn block(&self) -> &BlockId {
        &self.block
    }

    fn field_pos(&self, slot: i32, field_name: &str) -> Option<i32> {
        Some(self.offset(slot) + self.layout.offset(field_name)?)
    }

    fn set_is_used_flag(&mut self, slot: i32, flag: bool) -> Result<()> {
        self.set_flag(slot, IS_USED_FLAG_NAME, flag)
    }

    fn search_after(&mut self, slot: i32, flag: bool) -> Result<i32> {
        let mut next_slot = slot + 1;
        while self.is_valid_slot(next_slot) {
            if self.get_flag(next_slot, IS_USED_FLAG_NAME)? == flag {
                return Ok(next_slot);
            }
            next_slot += 1;
        }
        Ok(-1)
    }

    pub fn get_flag(&mut self, slot: i32, flag_name: &str) -> Result<bool> {
        let offset = self.offset(slot);
        let null_bit_location = self
            .layout
            .flag_bit_location(flag_name)
            .ok_or("flag not found")?;

        let tx = Arc::as_ptr(&self.tx) as *mut Transaction;
        let flag_bits = unsafe { (*tx).get_int(&self.block, offset)? };

        Ok((flag_bits & (1 << null_bit_location)) != 0)
    }
    fn set_flag(&mut self, slot: i32, flag_name: &str, flag: bool) -> Result<()> {
        let offset = self.offset(slot);
        let flag_bit_location = self
            .layout
            .flag_bit_location(flag_name)
            .ok_or("flag not found")?;

        let tx = Arc::as_ptr(&self.tx) as *mut Transaction;
        let mut flag_bits = unsafe { (*tx).get_int(&self.block, offset)? };
        if flag {
            flag_bits |= 1 << flag_bit_location;
        } else {
            flag_bits &= !(1 << flag_bit_location);
        }
        unsafe { (*tx).set_int(&self.block, offset, flag_bits, true) }
    }

    fn is_valid_slot(&self, slot: i32) -> bool {
        self.offset(slot + 1) <= self.tx.block_size()
    }

    fn offset(&self, slot: i32) -> i32 {
        self.layout.slot_size() * slot
    }
}
