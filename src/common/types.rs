pub type LocationIndex = u16;
pub type TransactionId = u32;
pub type PageSizeVersion = u16;
pub type CheckSum = u16;
pub type OffsetNumber = u16;
pub type HeapTupleData = Vec<u8>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RowId {
    pub page_id: u32,
    pub slot_num: u16,
}
