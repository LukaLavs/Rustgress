use crate::access::tuple::desc::{TupleDescriptor, Column};
use crate::utils::adt::datatype::{Value, DataType};
use crate::utils::adt::integer::IntegerType;
use crate::utils::adt::text::TextType;
use crate::access::tuple::tuple::HeapTuple;
use super::traits::RGSomething;

pub struct RGAttribute {
    pub attrelid: i32,        // table OID this column belongs to
    pub attname: String,      // row name
    pub atttypid: i32,        // type ID of the column
    pub attnum: i32,          // consecutive number of the column in the table
    pub attlen: i32,          // len of column data type (-1 for varlena types)
    // ...
}

impl RGSomething for RGAttribute {
    fn get_descriptor() -> TupleDescriptor {
        TupleDescriptor::new(vec![
            Column { name: "attrelid".to_string(), data_type: DataType::Integer },
            Column { name: "attname".to_string(), data_type: DataType::Text },
            Column { name: "atttypid".to_string(), data_type: DataType::Integer },
            Column { name: "attnum".to_string(), data_type: DataType::Integer },
            Column { name: "attlen".to_string(), data_type: DataType::Integer },
        ])
    }
    fn make_tuple(self, schema: &TupleDescriptor) -> HeapTuple {
        schema.pack(vec![
            Value::Integer(self.attrelid as i32),
            Value::Text(self.attname),
            Value::Integer(self.atttypid as i32),
            Value::Integer(self.attnum as i32),
            Value::Integer(self.attlen as i32),
        ])
    }
    fn from_tuple(tuple: &HeapTuple) -> Self {
        let schema = Self::get_descriptor();
        let values = schema.unpack_from_tuple(tuple);
        Self {
            attrelid: values[0].as_native::<IntegerType>().unwrap(),
            attname:  values[1].as_native::<TextType>().unwrap(),
            atttypid: values[2].as_native::<IntegerType>().unwrap(),
            attnum:   values[3].as_native::<IntegerType>().unwrap(),
            attlen:   values[4].as_native::<IntegerType>().unwrap(),
        }
    }
}

