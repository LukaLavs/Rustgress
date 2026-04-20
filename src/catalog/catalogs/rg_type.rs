use super::super::schema::{Schema, Column};
use super::super::types::{DataType, Value};
use crate::access::tuple::header::Tuple;
use super::traits::RGSomething;
use crate::common::constants::{RG_TYPE_OID};

pub struct RGType {
    pub oid: i32,           // unique type identifier
    pub typname: String,    // name of the type
    pub typlen: i32,        // fixed size in bytes, or -1 for variable length
    pub typbyval: bool,     // passed by value or by reference
}

impl RGSomething for RGType {
    fn get_schema() -> Schema {
        Schema::new(vec![
            Column { name: "oid".to_string(), data_type: DataType::Integer },
            Column { name: "typname".to_string(), data_type: DataType::Varchar(64) },
            Column { name: "typlen".to_string(), data_type: DataType::Integer }, // i16 shranimo kot Integer
            Column { name: "typbyval".to_string(), data_type: DataType::Boolean },
        ])
    }

    fn make_tuple(self, schema: &Schema) -> Tuple {
        schema.pack(vec![
            Value::Integer(self.oid),
            Value::Varchar(self.typname),
            Value::Integer(self.typlen as i32),
            Value::Boolean(self.typbyval),
        ])
    }

    fn from_tuple(tuple: &Tuple) -> Self {
            let schema = Self::get_schema();
            let values = schema.unpack_from_tuple(tuple);
            
            Self {
                oid:      values[0].as_i32().unwrap(),
                typname:  values[1].as_str().to_string(),
                typlen:   values[2].as_i32().unwrap(),
                typbyval: values[3].as_bool().unwrap(),
            }
        }
        
    fn get_oid() -> u32 { 
        RG_TYPE_OID 
    }
}