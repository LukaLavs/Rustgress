use crate::common::constants::{
    RG_TYPE_BOOL, RG_TYPE_DOUBLE, RG_TYPE_FLOAT, RG_TYPE_INT, 
    RG_TYPE_NUMERIC, RG_TYPE_TIMESTAMP, RG_TYPE_VARCHAR,
    RG_TYPE_LEN_BOOL, RG_TYPE_LEN_DOUBLE, RG_TYPE_LEN_FLOAT, RG_TYPE_LEN_INT,
    RG_TYPE_LEN_NUMERIC, RG_TYPE_LEN_TIMESTAMP, RG_TYPE_LEN_VARCHAR,
};


#[derive(Debug, Clone, PartialEq)] 
pub enum DataType { 
    Integer, 
    Varchar(u16),
    Boolean,
    Timestamp,
    Float,
    Double,
    Numeric(u8, u8), // precision and scale
}

impl DataType {
    pub fn unpack(&self, data: &[u8], cursor: &mut usize) -> Value {
        match self {
            DataType::Integer => {
                let val = i32::from_le_bytes(data[*cursor..*cursor+RG_TYPE_LEN_INT as usize].try_into().unwrap());
                *cursor += RG_TYPE_LEN_INT as usize;
                Value::Integer(val)
            }
            DataType::Boolean => {
                let val = data[*cursor] == 1;
                *cursor += RG_TYPE_LEN_BOOL as usize;
                Value::Boolean(val)
            }
            DataType::Float => {
                let val = f32::from_le_bytes(data[*cursor..*cursor+RG_TYPE_LEN_FLOAT as usize].try_into().unwrap());
                *cursor += RG_TYPE_LEN_FLOAT as usize;
                Value::Float(val)
            }
            DataType::Double | DataType::Timestamp => {
                let bytes = data[*cursor..*cursor+RG_TYPE_LEN_DOUBLE as usize].try_into().unwrap();
                *cursor += RG_TYPE_LEN_DOUBLE as usize;
                if let DataType::Double = self { 
                    Value::Double(f64::from_le_bytes(bytes)) 
                } else { 
                    Value::Timestamp(i64::from_le_bytes(bytes)) 
                }
            }
            DataType::Varchar(_) | DataType::Numeric(_, _) => {
                let len = u16::from_le_bytes(data[*cursor..*cursor+2].try_into().unwrap()) as usize;
                *cursor += 2;
                let s = std::str::from_utf8(&data[*cursor..*cursor+len]).unwrap().to_string();
                *cursor += len;
                if let DataType::Varchar(_) = self { Value::Varchar(s) } else { Value::Numeric(s) }
            }
        }
    }

    pub fn type_definitions() -> Vec<(u32, &'static str, i32, bool)> {
        vec![
            (RG_TYPE_INT, "integer", RG_TYPE_LEN_INT as i32, true),
            (RG_TYPE_VARCHAR, "varchar", RG_TYPE_LEN_VARCHAR as i32, false),
            (RG_TYPE_BOOL, "boolean", RG_TYPE_LEN_BOOL as i32, true),
            (RG_TYPE_TIMESTAMP, "timestamp", RG_TYPE_LEN_TIMESTAMP as i32, false),
            (RG_TYPE_FLOAT, "float", RG_TYPE_LEN_FLOAT as i32, true),
            (RG_TYPE_DOUBLE, "double", RG_TYPE_LEN_DOUBLE as i32, true),
            (RG_TYPE_NUMERIC, "numeric", RG_TYPE_LEN_NUMERIC as i32, false),
        ]
    }
}

impl DataType {
    pub fn get_oid(&self) -> i32 {
        match self {
            DataType::Integer => RG_TYPE_INT as i32,
            DataType::Varchar(_) => RG_TYPE_VARCHAR as i32,
            DataType::Boolean => RG_TYPE_BOOL as i32,
            DataType::Timestamp => RG_TYPE_TIMESTAMP as i32,
            DataType::Float => RG_TYPE_FLOAT as i32,
            DataType::Double => RG_TYPE_DOUBLE as i32,
            DataType::Numeric(_, _) => RG_TYPE_NUMERIC as i32,
        }
    }
    pub fn from_oid(oid: u32) -> DataType {
        match oid {
            RG_TYPE_INT => DataType::Integer,
            RG_TYPE_VARCHAR => DataType::Varchar(255), // default length
            RG_TYPE_BOOL => DataType::Boolean,
            RG_TYPE_TIMESTAMP => DataType::Timestamp,
            RG_TYPE_FLOAT => DataType::Float,
            RG_TYPE_DOUBLE => DataType::Double,
            RG_TYPE_NUMERIC => DataType::Numeric(10, 2), // default precision and scale
            _ => panic!("Unknown OID: {}", oid),
        }
    }
    pub fn get_byte_len(&self) -> i32 {
        match self {
            DataType::Integer => RG_TYPE_LEN_INT as i32,
            DataType::Varchar(_) => RG_TYPE_LEN_VARCHAR as i32,
            DataType::Boolean => RG_TYPE_LEN_BOOL as i32,
            DataType::Timestamp => RG_TYPE_LEN_TIMESTAMP as i32,
            DataType::Float => RG_TYPE_LEN_FLOAT as i32,
            DataType::Double => RG_TYPE_LEN_DOUBLE as i32,
            DataType::Numeric(_, _) => RG_TYPE_LEN_NUMERIC as i32,
        }
    }
}


#[derive(Debug, Clone, PartialEq)] 
pub enum Value { 
    Integer(i32), 
    Varchar(String),
    Boolean(bool),
    Timestamp(i64),
    Float(f32),
    Double(f64),
    Numeric(String),
    Null
}

impl Value {
    pub fn pack(&self, buffer: &mut Vec<u8>) {
        match self {
            Value::Integer(v) => buffer.extend_from_slice(&v.to_le_bytes()),
            Value::Boolean(b) => buffer.push(if *b { 1 } else { 0 }),
            Value::Timestamp(t) => buffer.extend_from_slice(&t.to_le_bytes()),
            Value::Float(f) => buffer.extend_from_slice(&f.to_le_bytes()),
            Value::Double(d) => buffer.extend_from_slice(&d.to_le_bytes()),
            Value::Varchar(s) | Value::Numeric(s) => {
                buffer.extend_from_slice(&(s.len() as u16).to_le_bytes());
                buffer.extend_from_slice(s.as_bytes());
            }
            Value::Null => {}
        }
    }
}

impl Value {
    pub fn as_str(&self) -> String {
        match self {
            Value::Integer(i) => i.to_string(),
            Value::Varchar(s) => s.clone(),
            Value::Boolean(b) => b.to_string(),
            Value::Timestamp(t) => t.to_string(),
            Value::Float(f) => f.to_string(),
            Value::Double(d) => d.to_string(),
            Value::Numeric(s) => s.clone(),
            Value::Null => "NULL".to_string(),
        }
    }
    pub fn as_i32(&self) -> Option<i32> {
        match self {
            Value::Integer(i) => Some(*i as i32),
            _ => None,
        }
    }
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Value::Boolean(b) => Some(*b),
            _ => None,
        }
    }
    pub fn as_f32(&self) -> Option<f32> {
        match self {
            Value::Float(f) => Some(*f),
            _ => None,
        }
    }
}
