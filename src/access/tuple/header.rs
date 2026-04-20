use crate::common::types::{TransactionId, HeapTupleData, RowId};
use zerocopy_derive::{IntoBytes, FromBytes, Immutable, KnownLayout};
use zerocopy::{IntoBytes, FromBytes};
use bitflags::bitflags;


pub mod item_id_flags {
    pub const LP_UNUSED: u8 = 0;
    pub const LP_NORMAL: u8 = 1;
    pub const LP_REDIRECT: u8 = 2;
    pub const LP_DEAD: u8 = 3;
}

#[repr(transparent)]
#[derive(IntoBytes, FromBytes, Immutable, KnownLayout, Debug, Copy, Clone)]
/// ItemIdData encodes the offset, length, and flags for a tuple in a page.
/// It is stored in the line pointer array of a page header and points to the actual tuple data in the page.
pub struct ItemIdData(pub u32);

impl ItemIdData {
    // First 15 bits: offset to tuple data, next 2 bits: flags, last 15 bits: length of tuple data
    pub fn new(off: u16, len: u16, flags: u8) -> Self {
        Self((off as u32 & 0x7FFF) | ((flags as u32 & 0x3) << 15) | ((len as u32 & 0x7FFF) << 17))
    }
    pub fn lp_off(&self) -> u16 { (self.0 & 0x7FFF) as u16 }
    pub fn lp_flags(&self) -> u8 { ((self.0 >> 15) & 0x3) as u8 }
    pub fn lp_len(&self) -> u16 { (self.0 >> 17) as u16 }
    pub fn set_lp_off(&mut self, off: u16) { self.0 = (self.0 & !0x7FFF) | (off as u32 & 0x7FFF) }
    /// Possible flags: LP_UNUSED, LP_NORMAL, LP_REDIRECT, LP_DEAD.
    pub fn set_lp_flags(&mut self, flags: u8) { self.0 = (self.0 & !(0x3 << 15)) | ((flags as u32 & 0x3) << 15) }
    /// Length of the tuple data in bytes.
    pub fn set_lp_len(&mut self, len: u16) { self.0 = (self.0 & !(0x7FFF << 17)) | ((len as u32 & 0x7FFF) << 17) }
}


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


pub struct Tuple {
    pub header: HeapTupleHeaderData,
    pub null_bitmap: Vec<u8>,
    pub data: HeapTupleData,
}

impl Tuple {
    /// Writes the tuple header, null bitmap, and data into the provided target buffer.
    pub fn serialize_into(&self, target_buffer: &mut [u8]) {
        let header_bytes = self.header.as_bytes();
        let hoff = self.header.t_hoff as usize;
        target_buffer[0..header_bytes.len()].copy_from_slice(header_bytes);
        if !self.null_bitmap.is_empty() {
            let bitmap_start = header_bytes.len();
            let bitmap_end = bitmap_start + self.null_bitmap.len();
            target_buffer[bitmap_start..bitmap_end].copy_from_slice(&self.null_bitmap);
        }
        let data_start = hoff;
        let data_end = data_start + self.data.len();
        target_buffer[data_start..data_end].copy_from_slice(&self.data);
    }
}


pub struct HeapTupleView<'a> {
    pub header: HeapTupleHeaderData,
    pub raw: &'a [u8], // raw bytes of the entire tuple (header + null bitmap + data)
}

impl<'a> HeapTupleView<'a> {
    pub fn new(raw_bytes: &'a [u8]) -> Self {
        let (header, _) = 
            HeapTupleHeaderData::read_from_prefix(raw_bytes)
            .expect("Raw bytes are too small to contain a valid tuple header");
        HeapTupleView { header, raw: raw_bytes }
    }
    pub fn header(&self) -> &HeapTupleHeaderData {
        &self.header
    }
    /// Returns the raw data bytes of the tuple (excluding the header and null bitmap).
    pub fn data(&self) -> &[u8] {
        &self.raw[self.header.t_hoff as usize..]
    }
    pub fn null_bitmap(&self) -> Option<&[u8]> {
        if !self.header.read_infomask().contains(TupleInfoMask::HEAP_HASNULL) {
            return None;
        }
        let header_size = std::mem::size_of::<HeapTupleHeaderData>();
        let hoff = self.header.t_hoff as usize;
        if hoff > header_size {
            Some(&self.raw[header_size..hoff])
        } else {
            None
        }
    }
}






