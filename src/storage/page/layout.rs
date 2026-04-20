use super::header::PageHeaderData;
use crate::access::tuple::header::{HeapTupleView, ItemIdData, Tuple, item_id_flags};
use crate::common::types::{BLCKSZ, LocationIndex, OffsetNumber, TransactionId, RowId};
use core::option::Option;
use zerocopy::{FromBytes, IntoBytes};

#[repr(align(8))]
pub struct Page {
    pub data: [u8; BLCKSZ],
}

impl Page {
    pub fn new(special_size: LocationIndex) -> Self {
        let mut page = Page {
            data: [0u8; BLCKSZ],
        };
        let page_header = PageHeaderData::new(special_size);
        let header_bytes = page_header.as_bytes();
        page.data[..header_bytes.len()].copy_from_slice(header_bytes);
        page
    }

    pub fn empty() -> Self {
        Page {
            data: [0u8; BLCKSZ],
        }
    }

    pub fn get_header(&self) -> PageHeaderData {
        let (header, _remainder) =
            PageHeaderData::read_from_prefix(&self.data).expect("Page data premejhen za glavo");
        header
    }

    pub fn set_header(&mut self, header: &PageHeaderData) {
        let bytes = header.as_bytes();
        self.data[..bytes.len()].copy_from_slice(bytes);
    }

    pub fn get_free_space(&self) -> usize {
        let h = self.get_header();
        (h.pd_upper.saturating_sub(h.pd_lower)) as usize
    }

    pub fn from_bytes(bytes: &[u8]) -> &Self {
        unsafe { &*(bytes.as_ptr() as *const Page) }
    } 
    pub fn get_tuple_bytes(&self, slot_num: OffsetNumber) -> Option<&[u8]> {
        if slot_num == 0 {
            return None;
        }
        let header_size = std::mem::size_of::<PageHeaderData>() as u16;
        let item_id_size = std::mem::size_of::<ItemIdData>() as u16;
        let offset_to_id = header_size + (slot_num - 1) * item_id_size;

        // Preveri, če smo še znotraj pd_lower (kje so shranjeni ItemId-ji)
        if offset_to_id + item_id_size > self.get_header().pd_lower {
            return None;
        }
        let item_id = unsafe {
            let ptr = self.data.as_ptr().add(offset_to_id as usize) as *const ItemIdData;
            std::ptr::read_unaligned(ptr)
        };
        if item_id.lp_flags() != item_id_flags::LP_NORMAL {
            return None;
        }
        let start = item_id.lp_off() as usize;
        let len = item_id.lp_len() as usize;
        let end = start + len;

        if end > BLCKSZ as usize {
            return None;
        }
        Some(&self.data[start..end])
    }

    /// Attempts to add a tuple to the page. Returns the offset (slot) number of the new tuple
    /// if successful, or None if there is not enough free space.
    pub fn add_tuple(&mut self, tuple: &Tuple) -> Option<OffsetNumber> {
        let item_id_size = std::mem::size_of::<ItemIdData>() as u16;
        let tuple_len = tuple.header.t_hoff as usize + tuple.data.len();
        let needed_space = tuple_len as u16 + item_id_size;
        if self.get_free_space() < needed_space as usize {
            return None;
        }

        let mut h = self.get_header();
        h.pd_upper -= tuple_len as u16;
        let lp_off = h.pd_upper;
        let lower_ptr_pos = h.pd_lower;
        h.pd_lower += item_id_size;
        self.set_header(&h);

        let item_id = ItemIdData::new(lp_off, tuple_len as u16, item_id_flags::LP_NORMAL);
        unsafe {
            let item_id_ptr = self.data.as_mut_ptr().add(lower_ptr_pos as usize) as *mut ItemIdData;
            std::ptr::write_unaligned(item_id_ptr, item_id);
        }
        let target_slice = &mut self.data[lp_off as usize..(lp_off as usize + tuple_len)];
        tuple.serialize_into(target_slice);
        let header_size = std::mem::size_of::<PageHeaderData>() as u16;
        Some((lower_ptr_pos - header_size) / item_id_size + 1)
    }

    pub fn get_tuple_view(&'_ self, slot_num: OffsetNumber) -> Option<HeapTupleView<'_>> {
        if slot_num == 0 {
            return None;
        }
        let header_size = std::mem::size_of::<PageHeaderData>() as u16;
        let item_id_size = std::mem::size_of::<ItemIdData>() as u16;
        let offset_to_id = header_size + (slot_num - 1) * item_id_size;
        if offset_to_id + item_id_size > self.get_header().pd_lower {
            return None;
        }
        let item_id = unsafe {
            let ptr = self.data.as_ptr().add(offset_to_id as usize) as *const ItemIdData;
            std::ptr::read_unaligned(ptr)
        };
        // Only return a view if the item is marked as normal (not deleted or redirected)
        if item_id.lp_flags() != item_id_flags::LP_NORMAL {
            return None;
        }
        let start = item_id.lp_off() as usize;
        let end = start + item_id.lp_len() as usize;
        let raw_tuple = &self.data[start..end];
        Some(HeapTupleView::new(raw_tuple))
    }
}


impl Page {
    fn get_tuple_offset(&self, slot_num: OffsetNumber) -> Option<usize> {
        if slot_num == 0 { return None; }
        let header_size = std::mem::size_of::<PageHeaderData>() as u16;
        let item_id_size = std::mem::size_of::<ItemIdData>() as u16;
        let offset_to_id = header_size + (slot_num - 1) * item_id_size;

        if offset_to_id + item_id_size > self.get_header().pd_lower { return None; }

        let item_id = unsafe {
            let ptr = self.data.as_ptr().add(offset_to_id as usize) as *const ItemIdData;
            std::ptr::read_unaligned(ptr)
        };

        if item_id.lp_flags() != item_id_flags::LP_NORMAL { return None; }
        Some(item_id.lp_off() as usize)
    }

    pub fn update_tuple_header<F>(&mut self, slot_num: OffsetNumber, f: F) -> bool 
    where F: FnOnce(&mut crate::access::tuple::header::HeapTupleHeaderData) 
    {
        if let Some(offset) = self.get_tuple_offset(slot_num) {
            let header_len = std::mem::size_of::<crate::access::tuple::header::HeapTupleHeaderData>();
            
            // Preberemo obstoječo glavo
            let mut header = {
                let bytes = &self.data[offset..offset + header_len];
                crate::access::tuple::header::HeapTupleHeaderData::read_from_prefix(bytes).unwrap().0
            };

            // Pokličemo closure, da spremeni polja
            f(&mut header);

            // Zapišemo nazaj
            self.data[offset..offset + header_len].copy_from_slice(header.as_bytes());
            return true;
        }
        false
    }

    pub fn set_xmax(&mut self, slot_num: OffsetNumber, xid: TransactionId) -> bool {
        self.update_tuple_header(slot_num, |h| {
            h.t_xmax = xid;
        })
    }

    pub fn set_ctid(&mut self, slot_num: OffsetNumber, new_rid: RowId) -> bool {
        self.update_tuple_header(slot_num, |h| {
            h.t_ctid_page = new_rid.page_id;
            h.t_ctid_slot = new_rid.slot_num as u16;
        })
    }

}