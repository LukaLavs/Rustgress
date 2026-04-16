pub const BLCKSZ: usize = 8192; // page size in bytes

pub type LocationIndex = u16;
pub type TransactionId = u32;
pub type PageSizeVersion = u16;
pub type CheckSum = u16;
pub type OffsetNumber = u16;
pub type HeapTupleData = Vec<u8>;

pub const PAGE_SIZE_VERSION: PageSizeVersion = (BLCKSZ as u16) << 8 | 0;
pub const CLOG_FILE_PATH: &str = "data/clog.dat";
