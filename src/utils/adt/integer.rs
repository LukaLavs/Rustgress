use crate::utils::adt::traits::TypeDescriptor;
use crate::common::constants::{RG_TYPE_INT, RG_TYPE_LEN_INT};

pub struct IntegerType;

impl TypeDescriptor for IntegerType {
    type Native = i32;
    const OID: u32 = RG_TYPE_INT;
    const NAME: &'static str = "INTEGER";
    const IS_FIXED: bool = true;
    const BYTE_LEN: i32 = RG_TYPE_LEN_INT;

    fn pack(val: &i32, buffer: &mut Vec<u8>) {
        buffer.extend_from_slice(&val.to_le_bytes());
    }

    fn unpack(data: &[u8], cursor: &mut usize) -> i32 {
        let val = i32::from_le_bytes(data[*cursor..*cursor+4].try_into().unwrap());
        *cursor += 4;
        val
    }
}