use super::schema::{Schema, Column};
use super::types::{DataType, Value};
use crate::access::tuple::header::Tuple;
use super::traits::RGSomething;

pub struct RGAttribute {
    pub attrelid: i32,        // table OID this column belongs to
    pub attname: String,      // row name
    pub atttypid: i32,        // type ID of the column
    pub attnum: i16,          // consecutive number of the column in the table
    pub attlen: i16,          // len of column data type (-1 for varlena types)
    // ...
}

impl RGSomething for RGAttribute {
    fn get_schema() -> Schema {
        Schema::new(vec![
            Column { name: "attrelid".to_string(), data_type: DataType::Integer },
            Column { name: "attname".to_string(), data_type: DataType::Varchar(64) },
            Column { name: "atttypid".to_string(), data_type: DataType::Integer },
            Column { name: "attnum".to_string(), data_type: DataType::Integer },
            Column { name: "attlen".to_string(), data_type: DataType::Integer },
        ])
    }
    fn make_tuple(&self, schema: &Schema) -> Tuple {
        schema.pack(vec![
            Value::Integer(self.attrelid as i32),
            Value::Varchar(self.attname.clone()),
            Value::Integer(self.atttypid as i32),
            Value::Integer(self.attnum as i32),
            Value::Integer(self.attlen as i32),
        ])
    }
    fn get_oid() -> u32 { 2 }
}

impl RGAttribute {
    pub fn new(
        col_type: DataType,
        attrelid: i32, attname: String, 
        attnum: i16) -> RGAttribute {
        RGAttribute {
            attrelid,
            attname,
            atttypid: col_type.get_oid(),
            attnum,
            attlen: col_type.get_byte_len(),
        }
    }
}
