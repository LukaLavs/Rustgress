use bitflags::bitflags;
use crate::common::types::{
    CheckSum, LocationIndex, PageSizeVersion, TransactionId,
    BLCKSZ, PAGE_SIZE_VERSION,
};
use zerocopy_derive::{FromBytes, Immutable, IntoBytes, KnownLayout};


bitflags! {
    #[repr(transparent)]
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct PageFlags: u16 {
        const HAS_FREE_LINES = 0x0001;
        const PAGE_FULL      = 0x0002;
        const ALL_VISIBLE    = 0x0004;
    }
}


#[derive(IntoBytes, FromBytes, KnownLayout, Debug, Copy, Clone, Immutable)]
#[repr(C, packed)]
/// pd_lower and pd_upper define the free space in the page. 
/// The area between pd_lower and pd_upper is free space.
/// pd_special defines the start of the special space at the end of the page, 
/// which is reserved for things like FSM or VM.
pub struct PageHeaderData { // 16 bytes
    pub pd_checksum: CheckSum, // checksum
    pub pd_flags: u16, // flag bits, see above
    pub pd_lower: LocationIndex, // offset to start of free space
    pub pd_upper: LocationIndex, // offset to end of free space
    pub pd_special: LocationIndex, // offset to start of special space
    pub pd_pagesize_version: PageSizeVersion, // page size and version info
    pub pd_prune_xid: TransactionId, // oldest prunable transaction
}

impl PageHeaderData {
    pub fn new(special: LocationIndex) -> Self {
        let header_size: LocationIndex = std::mem::size_of::<PageHeaderData>() as LocationIndex;
        let special_start = (BLCKSZ as u16).saturating_sub(special);
        PageHeaderData {
            pd_checksum: 0, // checksum will be calculated when writing the page to disk
            pd_flags: PageFlags::empty().bits(),
            pd_lower: header_size,
            pd_upper: special_start,
            pd_special: special_start,
            pd_pagesize_version: PAGE_SIZE_VERSION,
            pd_prune_xid: 0, // prunning not implemented yet
        }
    }
}

