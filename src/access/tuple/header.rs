use crate::common::types::{TransactionId, RowId};
use zerocopy_derive::{IntoBytes, FromBytes, Immutable, KnownLayout};
use bitflags::bitflags;


bitflags! {
    #[repr(transparent)]
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct TupleInfoMask: u16 {
        const HEAP_HASNULL        = 0x0001; // any null values in the tuple?
        const HEAP_HASVARWITHD    = 0x0002; // variable width attributes?
        const HEAP_HASEXTERNAL    = 0x0004; // any data TOASTed?
        const HEAP_HASOID         = 0x0008; // TODO: do we need that?
        const HEAP_XMIN_COMMITTED = 0x0100; // t_xmin is a committed transaction
        const HEAP_XMIN_INVALID   = 0x0200; // t_xmin is invalid or aborted
        const HEAP_XMAX_COMMITTED = 0x0400; // t_xmax is a committed transaction (deletion or update)
        const HEAP_XMAX_INVALID   = 0x0800; // t_xmax is invalid or aborted
        const HEAP_XMAX_IS_MULTI  = 0x1000;
    }
}

bitflags! {
    #[repr(transparent)]
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct TupleInfoMask2: u16 {
        const HEAP_NATTS_MASK   = 0x07FF; // number of attributes (columns) in the tuple, max 2047
        const HEAP_KEYS_UPDATED = 0x2000;
        const HEAP_HOT_UPDATED  = 0x4000;
        const HEAP_ONLY_TUPLE   = 0x8000;
    }
}

#[repr(C)]
#[derive(IntoBytes, FromBytes, Immutable, KnownLayout, Debug, Copy, Clone)]
pub struct HeapTupleHeaderData {
    pub t_xmin: TransactionId,   // id of inserting transaction
    pub t_xmax: TransactionId,   // id of deleting transaction, 0 if alive. Tuple is alive on [xmin, xmax) interval.
    pub t_ctid_page: u32,        // pointer to page for updates (if tuple is updated, this points to the new version)
    pub t_ctid_slot: u16,        // pointer to slot for updates (if tuple is updated, this points to the new version)
    pub t_infomask2: u16,        // number of attributes and flags
    pub t_infomask: u16,         // flags (e.g., HAS_NULL)
    pub t_padding: u8,           // padding byte for alignment
    pub t_hoff: u8,              // offset to actual data (including this header)
}

impl HeapTupleHeaderData {
    pub fn read_infomask(&self) -> TupleInfoMask {
        TupleInfoMask::from_bits_truncate(self.t_infomask)
    }
    pub fn read_infomask2(&self) -> TupleInfoMask2 {
        TupleInfoMask2::from_bits_truncate(self.t_infomask2)
    }
    pub fn num_attributes(&self) -> u16 {
        self.t_infomask2 & TupleInfoMask2::HEAP_NATTS_MASK.bits()
    }
    pub fn set_ctid(&mut self, page: u32, slot: u16) {
        self.t_ctid_page = page;
        self.t_ctid_slot = slot;
    }
    /// Check if this tuple version is the latest (i.e., not updated by another transaction)
    pub fn is_latest(&self, self_page: u32, self_slot: u16) -> bool {
        self.t_ctid_page == self_page && self.t_ctid_slot == self_slot
    }
    pub fn get_rid(&self) -> RowId {
        RowId {
            page_id: self.t_ctid_page,
            slot_num: self.t_ctid_slot,
        }
    }
}
