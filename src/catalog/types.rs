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
    pub fn get_oid(&self) -> i32 {
        match self {
            DataType::Integer => 1,
            DataType::Varchar(_) => 2,
            DataType::Boolean => 3,
            DataType::Timestamp => 4,
            DataType::Float => 5,
            DataType::Double => 6,
            DataType::Numeric(_, _) => 7,
        }
    }
    pub fn from_oid(oid: u32) -> DataType {
        match oid {
            1 => DataType::Integer,
            2 => DataType::Varchar(255), // default length
            3 => DataType::Boolean,
            4 => DataType::Timestamp,
            5 => DataType::Float,
            6 => DataType::Double,
            7 => DataType::Numeric(10, 2), // default precision and scale
            _ => panic!("Unknown OID: {}", oid),
        }
    }
    pub fn get_byte_len(&self) -> i16 {
        match self {
            DataType::Integer => 4,
            DataType::Varchar(_) => -1,
            DataType::Boolean => 1,
            DataType::Timestamp => 4, // TODO: ???
            DataType::Float => 4,
            DataType::Double => 8,
            DataType::Numeric(_, _) => -1,
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

