use super::types::PageSizeVersion;

pub const BLCKSZ: usize = 8192; // page size in bytes
pub const PAGE_SIZE_VERSION: PageSizeVersion = (BLCKSZ as u16) << 8 | 0;
pub const CLOG_FILE_PATH: &str = "data/clog.dat";

pub const RG_CLASS_OID: u32 = 1;
pub const RG_ATTRIBUTE_OID: u32 = 2;
pub const RG_NAMESPACE_OID: u32 = 3;
pub const RG_TYPE_OID: u32 = 4;


pub const RG_TYPE_INT: u32 = 50;
pub const RG_TYPE_VARCHAR: u32 = 51;
pub const RG_TYPE_BOOL: u32 = 52;
pub const RG_TYPE_TIMESTAMP: u32 = 53;
pub const RG_TYPE_FLOAT: u32 = 54;
pub const RG_TYPE_DOUBLE: u32 = 55;
pub const RG_TYPE_NUMERIC: u32 = 56;


pub const RG_TYPE_LEN_INT: i32 = 4;
pub const RG_TYPE_LEN_BOOL: i32 = 1;
pub const RG_TYPE_LEN_TIMESTAMP: i32 = 4;
pub const RG_TYPE_LEN_FLOAT: i32 = 4;
pub const RG_TYPE_LEN_DOUBLE: i32 = 8;
pub const RG_TYPE_LEN_VARCHAR: i32 = -1;
pub const RG_TYPE_LEN_NUMERIC: i32 = -1;


pub const USER_XID_START: u32 = 10000;
