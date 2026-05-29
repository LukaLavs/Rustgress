use crate::access::tuple::desc::{TupleDescriptor, Column};
use crate::utils::adt::datatype::{Value, DataType};
use crate::utils::adt::integer::IntegerType;
use crate::utils::adt::text::TextType;
use crate::utils::adt::boolean::BooleanType;
use crate::access::tuple::tuple::HeapTuple;
use super::traits::RGSomething;

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
            Column { name: "typname".to_string(), data_type: DataType::Text },
            Column { name: "typlen".to_string(), data_type: DataType::Integer }, // i16 shranimo kot Integer
            Column { name: "typbyval".to_string(), data_type: DataType::Boolean },
        ])
    }

    fn make_tuple(self, schema: &TupleDescriptor) -> HeapTuple {
        schema.pack(vec![
            Value::Integer(self.oid),
            Value::Text(self.typname),
            Value::Integer(self.typlen as i32),
            Value::Boolean(self.typbyval),
        ])
    }

    fn from_tuple(tuple: &HeapTuple) -> Self {
        let schema = Self::get_descriptor();
        let values = schema.unpack_from_tuple(tuple);
        
        Self {
            oid:      values[0].as_native::<IntegerType>().unwrap(),
            typname:  values[1].as_native::<TextType>().unwrap(),
            typlen:   values[2].as_native::<IntegerType>().unwrap(),
            typbyval: values[3].as_native::<BooleanType>().unwrap(),
        }
    }
}