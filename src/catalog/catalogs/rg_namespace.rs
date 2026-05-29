use crate::access::tuple::desc::{TupleDescriptor, Column};
use crate::utils::adt::datatype::{Value, DataType};
use crate::utils::adt::integer::IntegerType;
use crate::utils::adt::text::TextType;
use crate::access::tuple::tuple::HeapTuple;
use super::traits::RGSomething;

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
            Column { name: "nspname".to_string(), data_type: DataType::Text },
            Column { name: "nspowner".to_string(), data_type: DataType::Integer },
            Column { name: "nspacl".to_string(), data_type: DataType::Integer },
        ])
    }
    fn make_tuple(self, schema: &TupleDescriptor) -> HeapTuple {
        schema.pack(vec![
            Value::Integer(self.oid as i32),
            Value::Text(self.nspname),
            Value::Integer(self.nspowner as i32),
            Value::Integer(self.nspacl as i32),
        ])
    }
    fn from_tuple(tuple: &HeapTuple) -> Self {
        let schema = Self::get_descriptor();
        let values = schema.unpack_from_tuple(tuple);
        
        Self {
            oid:      values[0].as_native::<IntegerType>().unwrap(),
            nspname:  values[1].as_native::<TextType>().unwrap(),
            nspowner: values[2].as_native::<IntegerType>().unwrap(),
            nspacl:   values[3].as_native::<IntegerType>().unwrap(),
        }
    }
}