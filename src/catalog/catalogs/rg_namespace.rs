use crate::access::tuple::desc::{TupleDescriptor, Column};
use super::super::types::{DataType, Value};
use crate::access::tuple::tuple::HeapTuple;
use super::traits::RGSomething;
use crate::common::constants::SYSTEM_XID;

pub struct RGNamespace {
    pub oid: i32,             // unique table identifier
    pub nspname: String,      // schema name
    pub nspowner: i32,        // owner of the schema
    pub nspacl: i32,          // owner's permissions
}

impl RGSomething for RGNamespace {
    fn get_descriptor() -> TupleDescriptor {
        TupleDescriptor::new(vec![
            Column { name: "oid".to_string(), data_type: DataType::Integer },
            Column { name: "nspname".to_string(), data_type: DataType::Varchar(64) },
            Column { name: "nspowner".to_string(), data_type: DataType::Integer },
            Column { name: "nspacl".to_string(), data_type: DataType::Integer },
        ])
    }
    fn make_tuple(self, schema: &TupleDescriptor) -> HeapTuple {
        schema.pack(vec![
            Value::Integer(self.oid as i32),
            Value::Varchar(self.nspname),
            Value::Integer(self.nspowner as i32),
            Value::Integer(self.nspacl as i32),
        ], SYSTEM_XID)
    }
    fn from_tuple(tuple: &HeapTuple) -> Self {
        let schema = Self::get_descriptor();
        let values = schema.unpack_from_tuple(tuple);
        
        Self {
            oid:      values[0].as_i32().unwrap(),
            nspname:  values[1].as_str().to_string(),
            nspowner: values[2].as_i32().unwrap(),
            nspacl:   values[3].as_i32().unwrap(),
        }
    }
}