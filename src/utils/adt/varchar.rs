use crate::utils::adt::traits::TypeDescriptor;
use crate::common::constants::{RG_TYPE_VARCHAR, RG_TYPE_LEN_VARCHAR};

pub struct VarcharType;

impl TypeDescriptor for VarcharType {
    type Native = String;
    const OID: u32 = RG_TYPE_VARCHAR;
    const NAME: &'static str = "VARCHAR";
    const IS_FIXED: bool = false;
    const BYTE_LEN: i32 = RG_TYPE_LEN_VARCHAR; // Običajno -1 za variabilne tipe

    fn pack(val: &String, buffer: &mut Vec<u8>) {
        let bytes = val.as_bytes();
        buffer.extend_from_slice(&(bytes.len() as u16).to_le_bytes());
        buffer.extend_from_slice(bytes);
    }

    fn unpack(data: &[u8], cursor: &mut usize) -> String {
        let len = u16::from_le_bytes(data[*cursor..*cursor + 2].try_into().unwrap()) as usize;
        *cursor += 2;
        let s = std::str::from_utf8(&data[*cursor..*cursor + len]).unwrap().to_string();
        *cursor += len;
        s
    }
}