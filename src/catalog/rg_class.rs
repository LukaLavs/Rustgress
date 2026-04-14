use super::schema::{Schema, Column};
use super::types::{DataType, Value};
use crate::access::tuple::header::Tuple;

pub struct RGClass {
    pub oid: u32,             // unique table identifier
    pub relname: String,    // table name
    pub relnamespace: u32,    // which schema it belongs to
    pub relpages: u32,        // how many pages it occupies (for optimizer)
    pub reltuples: f32,       // approximate number of rows
    // ... perhaps more metadata should be added later
}

impl RGClass {
    pub fn get_schema() -> Schema {
        let schema = Schema::new(vec![
            Column { name: "oid".to_string(), data_type: DataType::Integer },
            Column { name: "relname".to_string(), data_type: DataType::Varchar(64) },
            Column { name: "relnamespace".to_string(), data_type: DataType::Integer },
            Column { name: "relpages".to_string(), data_type: DataType::Integer },
            Column { name: "reltuples".to_string(), data_type: DataType::Float },
        ]);
        schema
    }
    pub fn make_tuple(&self, schema: &Schema) -> Tuple {
        schema.pack(vec![
            Value::Integer(self.oid as i32),
            Value::Varchar(self.relname.clone()),
            Value::Integer(self.relnamespace as i32),
            Value::Integer(self.relpages as i32),
            Value::Float(self.reltuples),
        ])
    }
}

impl RGClass {
    pub fn new( // TODO: Later oid and relpages and reltuples should be read on its own
        oid: u32, relname: String, relnamespace: u32,
        relpages: u32, reltuples: f32) -> RGClass {
        RGClass { 
            oid, 
            relname, 
            relnamespace, 
            relpages, 
            reltuples,
        }
    }
}

 