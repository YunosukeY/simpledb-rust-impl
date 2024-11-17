#![allow(dead_code)]

use std::sync::Arc;

use crate::{
    file::block_id::BlockId,
    query::{constant::Constant, scan::Scan, update_scan::UpdateScan},
    sql::ColumnType,
    tx::transaction::Transaction,
    util::Result,
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
    pub fn new(tx: Arc<Transaction<'a>>, table_name: &str, layout: Arc<Layout>) -> Result<Self> {
        let mut scan = Self {
            tx,
            layout,
            rp: None,
            filename: format!("{}.tbl", table_name),
            current_slot: 0,
        };
        let tx = Arc::as_ptr(&scan.tx) as *mut Transaction;
        if unsafe { (*tx).size(&scan.filename)? == 0 } {
            scan.move_to_new_block()?;
        } else {
            scan.move_to_block(0)?;
        }
        Ok(scan)
    }
}

impl<'a> Scan for TableScan<'a> {
    fn before_first(&mut self) -> Result<()> {
        self.move_to_block(0)
    }

    fn next(&mut self) -> Result<bool> {
        self.current_slot = self.rp.as_mut().unwrap().next_after(self.current_slot)?;
        while self.current_slot < 0 {
            if self.at_last_block()? {
                return Ok(false);
            }
            self.move_to_block(self.rp.as_ref().unwrap().block().block_num() + 1)?;
            self.current_slot = self.rp.as_mut().unwrap().next_after(self.current_slot)?;
        }
        Ok(true)
    }

    fn get_int(&mut self, field_name: &str) -> Result<i32> {
        self.rp
            .as_mut()
            .unwrap()
            .get_int(self.current_slot, field_name)
    }

    fn get_double(&mut self, field_name: &str) -> Result<f64> {
        self.rp
            .as_mut()
            .unwrap()
            .get_double(self.current_slot, field_name)
    }

    fn get_bytes(&mut self, field_name: &str) -> Result<Vec<u8>> {
        self.rp
            .as_mut()
            .unwrap()
            .get_bytes(self.current_slot, field_name)
    }

    fn get_string(&mut self, field_name: &str) -> Result<String> {
        self.rp
            .as_mut()
            .unwrap()
            .get_string(self.current_slot, field_name)
    }

    fn get_boolean(&mut self, field_name: &str) -> Result<bool> {
        self.rp
            .as_mut()
            .unwrap()
            .get_bool(self.current_slot, field_name)
    }

    fn get_date(&mut self, field_name: &str) -> Result<chrono::NaiveDate> {
        self.rp
            .as_mut()
            .unwrap()
            .get_date(self.current_slot, field_name)
    }

    fn get_time(&mut self, field_name: &str) -> Result<chrono::NaiveTime> {
        self.rp
            .as_mut()
            .unwrap()
            .get_time(self.current_slot, field_name)
    }

    fn get_datetime(&mut self, field_name: &str) -> Result<chrono::DateTime<chrono::FixedOffset>> {
        self.rp
            .as_mut()
            .unwrap()
            .get_datetime(self.current_slot, field_name)
    }

    fn get_json(&mut self, field_name: &str) -> Result<serde_json::Value> {
        self.rp
            .as_mut()
            .unwrap()
            .get_json(self.current_slot, field_name)
    }

    fn get_value(&mut self, field_name: &str) -> Result<Constant> {
        let layout = Arc::as_ptr(&self.layout);
        let column_type = unsafe {
            (*layout)
                .schema()
                .column_type(field_name)
                .ok_or("field not found")?
        };
        match column_type {
            ColumnType::Integer => Ok(Constant::from(self.get_int(field_name)?)),
            ColumnType::Double => Ok(Constant::from(self.get_double(field_name)?)),
            ColumnType::VarBit => Ok(Constant::from(self.get_bytes(field_name)?)),
            ColumnType::VarChar => Ok(Constant::from(self.get_string(field_name)?)),
            ColumnType::Boolean => Ok(Constant::from(self.get_boolean(field_name)?)),
            ColumnType::Date => Ok(Constant::from(self.get_date(field_name)?)),
            ColumnType::Time => Ok(Constant::from(self.get_time(field_name)?)),
            ColumnType::DateTime => Ok(Constant::from(self.get_datetime(field_name)?)),
            ColumnType::Json => Ok(Constant::from(self.get_json(field_name)?)),
        }
    }

    fn is_null(&mut self, field_name: &str) -> Result<bool> {
        self.rp
            .as_mut()
            .unwrap()
            .is_null(self.current_slot, field_name)
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
    fn set_value(&mut self, field_name: &str, value: Constant) -> Result<()> {
        let layout = Arc::as_ptr(&self.layout);
        let column_type = unsafe {
            (*layout)
                .schema()
                .column_type(field_name)
                .ok_or("field not found")?
        };
        match column_type {
            ColumnType::Integer => self.set_int(field_name, value.as_int().ok_or("invalid value")?),
            ColumnType::Double => {
                self.set_double(field_name, value.as_double().ok_or("invalid value")?)
            }
            ColumnType::VarBit => {
                self.set_bytes(field_name, value.as_bytes().ok_or("invalid value")?)
            }
            ColumnType::VarChar => {
                self.set_string(field_name, value.as_string().ok_or("invalid value")?)
            }
            ColumnType::Boolean => {
                self.set_boolean(field_name, value.as_boolean().ok_or("invalid value")?)
            }
            ColumnType::Date => self.set_date(field_name, value.as_date().ok_or("invalid value")?),
            ColumnType::Time => self.set_time(field_name, value.as_time().ok_or("invalid value")?),
            ColumnType::DateTime => {
                self.set_datetime(field_name, value.as_datetime().ok_or("invalid value")?)
            }
            ColumnType::Json => self.set_json(field_name, value.as_json().ok_or("invalid value")?),
        };
        Ok(())
    }

    fn set_int(&mut self, field_name: &str, value: i32) -> &mut Self {
        let _ = self
            .rp
            .as_mut()
            .unwrap()
            .set_int(self.current_slot, field_name, value);
        self
    }

    fn set_double(&mut self, field_name: &str, value: f64) -> &mut Self {
        let _ = self
            .rp
            .as_mut()
            .unwrap()
            .set_double(self.current_slot, field_name, value);
        self
    }

    fn set_bytes(&mut self, field_name: &str, value: &[u8]) -> &mut Self {
        let _ = self
            .rp
            .as_mut()
            .unwrap()
            .set_bytes(self.current_slot, field_name, value);
        self
    }

    fn set_string(&mut self, field_name: &str, value: &str) -> &mut Self {
        let _ = self
            .rp
            .as_mut()
            .unwrap()
            .set_string(self.current_slot, field_name, value);
        self
    }

    fn set_boolean(&mut self, field_name: &str, value: bool) -> &mut Self {
        let _ = self
            .rp
            .as_mut()
            .unwrap()
            .set_bool(self.current_slot, field_name, value);
        self
    }

    fn set_date(&mut self, field_name: &str, value: chrono::NaiveDate) -> &mut Self {
        let _ = self
            .rp
            .as_mut()
            .unwrap()
            .set_date(self.current_slot, field_name, value);
        self
    }

    fn set_time(&mut self, field_name: &str, value: chrono::NaiveTime) -> &mut Self {
        let _ = self
            .rp
            .as_mut()
            .unwrap()
            .set_time(self.current_slot, field_name, value);
        self
    }

    fn set_datetime(
        &mut self,
        field_name: &str,
        value: chrono::DateTime<chrono::FixedOffset>,
    ) -> &mut Self {
        let _ = self
            .rp
            .as_mut()
            .unwrap()
            .set_datetime(self.current_slot, field_name, value);
        self
    }

    fn set_json(&mut self, field_name: &str, value: &serde_json::Value) -> &mut Self {
        let _ = self
            .rp
            .as_mut()
            .unwrap()
            .set_json(self.current_slot, field_name, value);
        self
    }

    fn set_null(&mut self, field_name: &str) -> &mut Self {
        let _ = self
            .rp
            .as_mut()
            .unwrap()
            .set_null(self.current_slot, field_name);
        self
    }

    fn insert(&mut self) -> Result<()> {
        self.current_slot = self.rp.as_mut().unwrap().insert_after(self.current_slot)?;
        while self.current_slot < 0 {
            if self.at_last_block()? {
                self.move_to_new_block()?;
            } else {
                self.move_to_block(self.rp.as_ref().unwrap().block().block_num() + 1)?;
            }
            self.current_slot = self.rp.as_mut().unwrap().insert_after(self.current_slot)?;
        }
        Ok(())
    }

    fn delete(&mut self) -> Result<()> {
        self.rp.as_mut().unwrap().delete(self.current_slot)
    }

    fn get_rid(&self) -> Rid {
        let block_num = self.rp.as_ref().unwrap().block().block_num();
        Rid::new(block_num, self.current_slot)
    }

    fn move_to_rid(&mut self, rid: Rid) -> Result<()> {
        self.close();
        let block = BlockId::new(self.filename.clone(), rid.block_num());
        self.rp = Some(RecordPage::new(
            self.tx.clone(),
            block,
            self.layout.clone(),
        )?);
        self.current_slot = rid.slot();
        Ok(())
    }
}

impl<'a> TableScan<'a> {
    fn move_to_block(&mut self, block_num: i32) -> Result<()> {
        self.close();
        let block = BlockId::new(self.filename.clone(), block_num);
        self.rp = Some(RecordPage::new(
            self.tx.clone(),
            block,
            self.layout.clone(),
        )?);
        self.current_slot = -1;
        Ok(())
    }

    fn move_to_new_block(&mut self) -> Result<()> {
        self.close();
        let tx = Arc::as_ptr(&self.tx) as *mut Transaction;
        let block = unsafe { (*tx).append(&self.filename)? };
        self.rp = Some(RecordPage::new(
            self.tx.clone(),
            block,
            self.layout.clone(),
        )?);
        self.rp.as_mut().unwrap().format()?;
        self.current_slot = -1;
        Ok(())
    }

    fn at_last_block(&mut self) -> Result<bool> {
        let block_num = self.rp.as_ref().unwrap().block().block_num();
        let tx = Arc::as_ptr(&self.tx) as *mut Transaction;
        unsafe { Ok(block_num == (*tx).size(&self.filename)? - 1) }
    }
}

#[cfg(test)]
mod tests {
    use crate::{record::schema::Schema, server::simple_db::SimpleDB};

    use super::*;

    #[test]
    fn test() {
        let db = SimpleDB::new("testdata/record/table_scan/test", 4096, 8, "templog");
        let tx = Arc::new(db.new_tx());
        let mut schema = Schema::new();
        schema
            .add_int_field("int")
            .add_double_field("double")
            .add_bytes_field("bytes", 10)
            .add_string_field("string", 10)
            .add_boolean_field("boolean")
            .add_date_field("date")
            .add_time_field("time")
            .add_datetime_field("datetime")
            .add_json_field("json", 30);
        let layout = Arc::new(Layout::from(schema));
        let mut scan = TableScan::new(tx, "temp", layout).unwrap();
        let data = vec![
            (
                1,
                1.1,
                b"foo",
                "foo",
                true,
                chrono::NaiveDate::from_ymd_opt(1, 1, 1).unwrap(),
                chrono::NaiveTime::from_hms_opt(1, 1, 1).unwrap(),
                chrono::DateTime::parse_from_rfc3339("0001-01-01 01:01:01Z").unwrap(),
                serde_json::json!({"k": "v1"}),
            ),
            (
                2,
                2.2,
                b"bar",
                "bar",
                false,
                chrono::NaiveDate::from_ymd_opt(2, 2, 2).unwrap(),
                chrono::NaiveTime::from_hms_opt(2, 2, 2).unwrap(),
                chrono::DateTime::parse_from_rfc3339("0002-02-02 02:02:02Z").unwrap(),
                serde_json::json!({"k": "v2"}),
            ),
            (
                3,
                3.3,
                b"baz",
                "baz",
                true,
                chrono::NaiveDate::from_ymd_opt(3, 3, 3).unwrap(),
                chrono::NaiveTime::from_hms_opt(3, 3, 3).unwrap(),
                chrono::DateTime::parse_from_rfc3339("0003-03-03 03:03:03Z").unwrap(),
                serde_json::json!({"k": "v3"}),
            ),
        ];

        for (int, double, bytes, string, boolean, date, time, datetime, json) in data.clone() {
            scan.insert().unwrap();
            scan.set_int("int", int)
                .set_double("double", double)
                .set_bytes("bytes", bytes)
                .set_string("string", string)
                .set_boolean("boolean", boolean)
                .set_date("date", date)
                .set_time("time", time)
                .set_datetime("datetime", datetime)
                .set_json("json", &json);
        }

        scan.before_first().unwrap();

        for (int, double, bytes, string, boolean, date, time, datetime, json) in data {
            assert!(scan.next().unwrap());
            assert!(scan.get_int("int").unwrap() == int);
            assert!(scan.get_double("double").unwrap() == double);
            assert!(scan.get_bytes("bytes").unwrap() == bytes);
            assert!(scan.get_string("string").unwrap() == string);
            assert!(scan.get_boolean("boolean").unwrap() == boolean);
            assert!(scan.get_date("date").unwrap() == date);
            assert!(scan.get_time("time").unwrap() == time);
            assert!(scan.get_datetime("datetime").unwrap() == datetime);
            assert!(scan.get_json("json").unwrap() == json);
        }
        assert!(!scan.next().unwrap());
    }
}
