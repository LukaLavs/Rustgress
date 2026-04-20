use super::super::schema::{Schema, Column};
use super::super::types::{DataType, Value};
use crate::access::tuple::header::Tuple;
use super::traits::RGSomething;
use crate::common::constants::{RG_ATTRIBUTE_OID};

pub struct RGAttribute {
    pub attrelid: i32,        // table OID this column belongs to
    pub attname: String,      // row name
    pub atttypid: i32,        // type ID of the column
    pub attnum: i32,          // consecutive number of the column in the table
    pub attlen: i32,          // len of column data type (-1 for varlena types)
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
    fn make_tuple(self, schema: &Schema) -> Tuple {
        schema.pack(vec![
            Value::Integer(self.attrelid as i32),
            Value::Varchar(self.attname),
            Value::Integer(self.atttypid as i32),
            Value::Integer(self.attnum as i32),
            Value::Integer(self.attlen as i32),
        ])
    }
    fn from_tuple(tuple: &Tuple) -> Self {
        let schema = Self::get_schema();
        let values = schema.unpack_from_tuple(tuple);
        Self {
            attrelid: values[0].as_i32().unwrap(),
            attname:  values[1].as_str().to_string(),
            atttypid: values[2].as_i32().unwrap(),
            attnum:   values[3].as_i32().unwrap(),
            attlen:   values[4].as_i32().unwrap(),
        }
    }
    fn get_oid() -> u32 { RG_ATTRIBUTE_OID }
}

