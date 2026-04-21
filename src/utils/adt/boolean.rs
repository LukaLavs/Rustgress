use crate::utils::adt::traits::TypeDescriptor;
use crate::common::constants::{RG_TYPE_BOOL, RG_TYPE_LEN_BOOL};

pub struct BooleanType;

impl TypeDescriptor for BooleanType {
    type Native = bool;
    const OID: u32 = RG_TYPE_BOOL;
    const NAME: &'static str = "BOOLEAN";
    const IS_FIXED: bool = true;
    const BYTE_LEN: i32 = RG_TYPE_LEN_BOOL;

    fn pack(val: &bool, buffer: &mut Vec<u8>) {
        buffer.push(if *val { 1 } else { 0 });
    }

    fn unpack(data: &[u8], cursor: &mut usize) -> bool {
        let val = data[*cursor] == 1;
        *cursor += 1;
        val
    }
}