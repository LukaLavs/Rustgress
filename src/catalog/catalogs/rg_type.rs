use crate::access::tuple::desc::{TupleDescriptor, Column};
use super::super::types::{DataType, Value};
use crate::access::tuple::tuple::HeapTuple;
use super::traits::RGSomething;
use crate::common::constants::SYSTEM_XID;

pub struct RGType {
    pub oid: i32,           // unique type identifier
    pub typname: String,    // name of the type
    pub typlen: i32,        // fixed size in bytes, or -1 for variable length
    pub typbyval: bool,     // passed by value or by reference
}

impl RGSomething for RGType {
    fn get_descriptor() -> TupleDescriptor {
        TupleDescriptor::new(vec![
            Column { name: "oid".to_string(), data_type: DataType::Integer },
            Column { name: "typname".to_string(), data_type: DataType::Varchar(64) },
            Column { name: "typlen".to_string(), data_type: DataType::Integer }, // i16 shranimo kot Integer
            Column { name: "typbyval".to_string(), data_type: DataType::Boolean },
        ])
    }

    fn make_tuple(self, schema: &TupleDescriptor) -> HeapTuple {
        schema.pack(vec![
            Value::Integer(self.oid),
            Value::Varchar(self.typname),
            Value::Integer(self.typlen as i32),
            Value::Boolean(self.typbyval),
        ], SYSTEM_XID)
    }

    fn from_tuple(tuple: &HeapTuple) -> Self {
        let schema = Self::get_descriptor();
        let values = schema.unpack_from_tuple(tuple);
        
        Self {
            oid:      values[0].as_i32().unwrap(),
            typname:  values[1].as_str().to_string(),
            typlen:   values[2].as_i32().unwrap(),
            typbyval: values[3].as_bool().unwrap(),
        }
    }
}