use crate::utils::adt::traits::TypeDescriptor;
use crate::common::constants::{RG_TYPE_DOUBLE, RG_TYPE_LEN_DOUBLE};

pub struct DoubleType;

impl TypeDescriptor for DoubleType {
    type Native = f64;
    const OID: u32 = RG_TYPE_DOUBLE;
    const NAME: &'static str = "DOUBLE";
    const IS_FIXED: bool = true;
    const BYTE_LEN: i32 = RG_TYPE_LEN_DOUBLE;

    fn pack(val: &f64, buffer: &mut Vec<u8>) {
        buffer.extend_from_slice(&val.to_le_bytes());
    }

    fn unpack(data: &[u8], cursor: &mut usize) -> f64 {
        let val = f64::from_le_bytes(data[*cursor..*cursor + 8].try_into().unwrap());
        *cursor += 8;
        val
    }
}