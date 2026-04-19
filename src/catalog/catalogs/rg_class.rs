use super::super::schema::{Schema, Column};
use super::super::types::{DataType, Value};
use crate::access::tuple::header::Tuple;
use super::traits::RGSomething;
use crate::common::constants::{RG_CLASS_OID};

pub struct RGClass {
    pub oid: i32,             // unique table identifier
    pub relname: String,    // table name
    pub relnamespace: i32,    // which schema it belongs to
    pub relpages: i32,        // how many pages it occupies (for optimizer)
    pub reltuples: f32,       // approximate number of rows
    pub relspecial: i32,      // special size (depends on table type, 0 for regulars)
    pub relnatts: i32,       // number of attributes (columns) in the table
    // ... perhaps more metadata should be added later
}

impl RGSomething for RGClass {
    fn get_schema() -> Schema {
        let schema = Schema::new(vec![
            Column { name: "oid".to_string(), data_type: DataType::Integer },
            Column { name: "relname".to_string(), data_type: DataType::Varchar(64) },
            Column { name: "relnamespace".to_string(), data_type: DataType::Integer },
            Column { name: "relpages".to_string(), data_type: DataType::Integer },
            Column { name: "reltuples".to_string(), data_type: DataType::Float },
            Column { name: "relspecial".to_string(), data_type: DataType::Integer },
            Column { name: "relnatts".to_string(), data_type: DataType::Integer }, 
        ]);
        schema
    }
    fn make_tuple(self, schema: &Schema) -> Tuple {
        schema.pack(vec![
            Value::Integer(self.oid),
            Value::Varchar(self.relname),
            Value::Integer(self.relnamespace),
            Value::Integer(self.relpages),
            Value::Float(self.reltuples),
            Value::Integer(self.relspecial),
            Value::Integer(self.relnatts),
        ])
    }
    fn get_oid() -> u32 { RG_CLASS_OID }
}


 