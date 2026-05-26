use crate::utils::debug::errors::PageError;

use crate::storage::page::page::Page;
use crate::access::tuple::header::HeapTupleHeaderData;
use crate::access::tuple::tuple::HeapTuple;
use crate::common::types::{OffsetNumber, TransactionId, RowId};
use super::header::{TupleInfoMask, TupleInfoMask2};


pub trait HeapPageExt {
    fn heap_add_tuple(&mut self, tuple: &mut HeapTuple) -> Result<OffsetNumber, PageError>;
    fn heap_set_xmax(&mut self, slot_num: OffsetNumber, xid: TransactionId) -> Result<(), PageError>;
    fn heap_set_ctid(&mut self, slot_num: OffsetNumber, new_rid: RowId) -> Result<(), PageError>;
    fn heap_update_infomask(&mut self, slot_num: OffsetNumber, to_add: TupleInfoMask, to_remove: TupleInfoMask) -> Result<(), PageError>;
    fn heap_update_infomask2(&mut self, slot_num: OffsetNumber, to_add: TupleInfoMask2, to_remove: TupleInfoMask2) -> Result<(), PageError>;
}

impl HeapPageExt for Page {
    fn heap_add_tuple(&mut self, tuple: &mut HeapTuple) -> Result<OffsetNumber, PageError> {
        self.add_item::<HeapTuple>(tuple)
    }

    fn heap_set_xmax(&mut self, slot_num: OffsetNumber, xid: TransactionId) -> Result<(), PageError> {
        self.update_item_header::<HeapTupleHeaderData, _>(slot_num, |h| {
            h.t_xmax = xid;
        })
    }

    fn heap_set_ctid(&mut self, slot_num: OffsetNumber, new_rid: RowId) -> Result<(), PageError> {
        self.update_item_header::<HeapTupleHeaderData, _>(slot_num, |h| {
            h.t_ctid_page = new_rid.page_id;
            h.t_ctid_slot = new_rid.slot_num as u16;
        })
    }
    fn heap_update_infomask(&mut self, slot_num: OffsetNumber, to_add: TupleInfoMask, to_remove: TupleInfoMask) -> Result<(), PageError> {
        self.update_item_header::<HeapTupleHeaderData, _>(slot_num, |h| {
            h.update_infomask(to_add, to_remove);
        })
    }
    fn heap_update_infomask2(&mut self, slot_num: OffsetNumber, to_add: TupleInfoMask2, to_remove: TupleInfoMask2) -> Result<(), PageError> {
        self.update_item_header::<HeapTupleHeaderData, _>(slot_num, |h| {
            h.update_infomask2(to_add, to_remove);
        })
    }
}
