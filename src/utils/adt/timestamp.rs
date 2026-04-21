use crate::utils::adt::traits::TypeDescriptor;
use crate::common::constants::{RG_TYPE_TIMESTAMP, RG_TYPE_LEN_TIMESTAMP};

pub struct TimestampType;

impl TypeDescriptor for TimestampType {
    type Native = i64;
    const OID: u32 = RG_TYPE_TIMESTAMP;
    const NAME: &'static str = "TIMESTAMP";
    const IS_FIXED: bool = true;
    const BYTE_LEN: i32 = RG_TYPE_LEN_TIMESTAMP;

    fn pack(val: &i64, buffer: &mut Vec<u8>) {
        buffer.extend_from_slice(&val.to_le_bytes());
    }

    fn unpack(data: &[u8], cursor: &mut usize) -> i64 {
        let val = i64::from_le_bytes(data[*cursor..*cursor + 8].try_into().unwrap());
        *cursor += 8;
        val
    }
}