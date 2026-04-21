use crate::storage::page::page::Page;
use crate::access::tuple::header::HeapTupleHeaderData;
use crate::access::tuple::tuple::HeapTuple;
use crate::common::types::{OffsetNumber, TransactionId, RowId};
use crate::storage::page::header::PageHeaderData;
use crate::storage::page::item::{ItemIdData};


// use crate::storage::page::page::{Page, PageItem};
// use crate::access::tuple::header::HeapTupleHeaderData;
// use crate::access::tuple::tuple::HeapTuple;
// use crate::common::types::{OffsetNumber, TransactionId, RowId};
// use crate::storage::page::header::PageHeaderData;
// use crate::storage::page::item::ItemIdData;

pub trait HeapPageExt {
    fn heap_add_tuple(&mut self, tuple: &mut HeapTuple) -> Option<OffsetNumber>;
    fn heap_set_xmax(&mut self, slot_num: OffsetNumber, xid: TransactionId);
    fn heap_set_ctid(&mut self, slot_num: OffsetNumber, new_rid: RowId);
}

impl HeapPageExt for Page {
    fn heap_add_tuple(&mut self, tuple: &mut HeapTuple) -> Option<OffsetNumber> {
        self.add_item::<HeapTuple>(tuple)
    }

    fn heap_set_xmax(&mut self, slot_num: OffsetNumber, xid: TransactionId) {
        self.update_item_header::<HeapTupleHeaderData, _>(slot_num, |h| {
            h.t_xmax = xid;
        })
    }

    fn heap_set_ctid(&mut self, slot_num: OffsetNumber, new_rid: RowId) {
        self.update_item_header::<HeapTupleHeaderData, _>(slot_num, |h| {
            h.t_ctid_page = new_rid.page_id;
            h.t_ctid_slot = new_rid.slot_num as u16;
        })
    }
}

// impl Page {

//     pub fn insert_tuple(&mut self, tuple: &Tuple) {
//         let mut last_page_id = self.num_pages().saturating_sub(1);
//         let mut page = if self.num_pages() == 0 { last_page_id = 0; Page::new(0) } else { self.read_page(last_page_id) };
//         if page.add_tuple(tuple).is_some() {
//             self.write_page(last_page_id, &page);
//         } else {
//             let mut new_page = Page::new(0);
//             new_page.add_tuple(tuple);
//             self.write_page(last_page_id + 1, &new_page);
//         }
//     }
//     pub fn delete_tuple(&mut self, page_id: u32, slot_num: OffsetNumber) {
//         let mut page = self.read_page(page_id);
        
//         let header_size = std::mem::size_of::<PageHeaderData>() as u16;
//         let item_id_size = std::mem::size_of::<ItemIdData>() as u16;
//         let offset_to_id = header_size + (slot_num - 1) * item_id_size;

//         if offset_to_id + item_id_size <= page.get_header().pd_lower {
//             unsafe {
//                 let item_id_ptr = page.data.as_mut_ptr().add(offset_to_id as usize) as *mut ItemIdData;
//                 let mut item_id = ptr::read_unaligned(item_id_ptr);

//                 item_id.set_lp_flags(item_id_flags::LP_DEAD);
                
//                 ptr::write_unaligned(item_id_ptr, item_id);
//             }
//             self.write_page(page_id, &page);
//         }
//     }
//     pub fn update_tuple(&mut self, page_id: u32, slot_num: OffsetNumber, new_tuple: &HeapTuple) {
//         self.delete_tuple(page_id, slot_num); // delete old tuple (mark as dead)

//         self.insert_tuple(new_tuple); // insert new tuple
//     }
// }