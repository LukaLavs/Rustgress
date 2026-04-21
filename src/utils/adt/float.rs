use crate::utils::adt::traits::TypeDescriptor;
use crate::common::constants::{RG_TYPE_FLOAT, RG_TYPE_LEN_FLOAT};

pub struct FloatType;

impl TypeDescriptor for FloatType {
    type Native = f32;
    const OID: u32 = RG_TYPE_FLOAT;
    const NAME: &'static str = "FLOAT";
    const IS_FIXED: bool = true;
    const BYTE_LEN: i32 = RG_TYPE_LEN_FLOAT;

    fn pack(val: &f32, buffer: &mut Vec<u8>) {
        buffer.extend_from_slice(&val.to_le_bytes());
    }

    fn unpack(data: &[u8], cursor: &mut usize) -> f32 {
        let val = f32::from_le_bytes(data[*cursor..*cursor + 4].try_into().unwrap());
        *cursor += 4;
        val
    }
}