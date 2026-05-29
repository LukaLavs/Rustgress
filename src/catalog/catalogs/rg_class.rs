use crate::access::tuple::desc::{TupleDescriptor, Column};
use crate::utils::adt::datatype::{Value, DataType};
use crate::utils::adt::integer::IntegerType;
use crate::utils::adt::text::TextType;
use crate::utils::adt::float::FloatType;
use crate::access::tuple::tuple::HeapTuple;
use super::traits::RGSomething;

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
    fn get_descriptor() -> TupleDescriptor {
        TupleDescriptor::new(vec![
            Column { name: "oid".to_string(), data_type: DataType::Integer },
            Column { name: "relname".to_string(), data_type: DataType::Text },
            Column { name: "relnamespace".to_string(), data_type: DataType::Integer },
            Column { name: "relpages".to_string(), data_type: DataType::Integer },
            Column { name: "reltuples".to_string(), data_type: DataType::Float },
            Column { name: "relspecial".to_string(), data_type: DataType::Integer },
            Column { name: "relnatts".to_string(), data_type: DataType::Integer }, 
        ])
    }
    fn make_tuple(self, schema: &TupleDescriptor) -> HeapTuple {
        schema.pack(vec![
            Value::Integer(self.oid),
            Value::Text(self.relname),
            Value::Integer(self.relnamespace),
            Value::Integer(self.relpages),
            Value::Float(self.reltuples),
            Value::Integer(self.relspecial),
            Value::Integer(self.relnatts),
        ])
    }
    fn from_tuple(tuple: &HeapTuple) -> Self {
        let schema = Self::get_descriptor();
        let values = schema.unpack_from_tuple(tuple);
        
        Self {
            oid:      values[0].as_native::<IntegerType>().unwrap(),
            relname:  values[1].as_native::<TextType>().unwrap(),
            relnamespace: values[2].as_native::<IntegerType>().unwrap(),
            relpages: values[3].as_native::<IntegerType>().unwrap(),
            reltuples: values[4].as_native::<FloatType>().unwrap(),
            relspecial: values[5].as_native::<IntegerType>().unwrap(),
            relnatts: values[6].as_native::<IntegerType>().unwrap(),
        }
    }
}


 