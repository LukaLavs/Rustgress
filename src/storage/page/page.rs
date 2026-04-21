use super::header::PageHeaderData;
use super::item::{PageItem, ItemIdData, item_id_flags};
use crate::common::types::{LocationIndex, OffsetNumber};
use crate::common::constants::{BLCKSZ};
use core::option::Option;
use zerocopy::{FromBytes, IntoBytes, KnownLayout};

#[repr(align(8))]
pub struct Page {
    pub data: [u8; BLCKSZ],
}

impl Page {
    pub(crate) fn new(special_size: LocationIndex) -> Self {
        let mut page = Page {
            data: [0u8; BLCKSZ],
        };
        let page_header = PageHeaderData::new(special_size);
        let header_bytes = page_header.as_bytes();
        page.data[..header_bytes.len()].copy_from_slice(header_bytes);
        page
    }

    /// Creates a new page with all bytes set to zero.
    pub(crate) fn empty() -> Self {
        Page {
            data: [0u8; BLCKSZ],
        }
    }

    pub(crate) fn get_header(&self) -> PageHeaderData {
        let (header, _remainder) =
            PageHeaderData::read_from_prefix(&self.data)
            .expect("Page data is too small to contain a valid header.");
        header
    }

    pub(crate) fn set_header(&mut self, header: &PageHeaderData) {
        let bytes = header.as_bytes();
        self.data[..bytes.len()].copy_from_slice(bytes);
    }

    pub(crate) fn get_free_space(&self) -> usize {
        let h = self.get_header();
        (h.pd_upper.saturating_sub(h.pd_lower)) as usize
    }

    pub(crate) fn from_bytes(bytes: &[u8]) -> &Self {
        unsafe { &*(bytes.as_ptr() as *const Page) }
    } 

    fn get_item_id(&self, slot_num: OffsetNumber) -> Option<ItemIdData> {
        if slot_num == 0 { return None; }
        let header_size = std::mem::size_of::<PageHeaderData>();
        let item_size = std::mem::size_of::<ItemIdData>();
        let offset = header_size + (slot_num as usize - 1) * item_size;
        if offset + item_size > self.get_header().pd_lower as usize {
            return None;
        }
        ItemIdData::read_from_prefix(&self.data[offset..])
            .ok().map(|(id, _)| id)
    }

    /// Attempts to add a item to the page. Returns the offset (slot) number to the new item
    /// if successful, or None if there is not enough free space.
    pub(crate) fn add_item<T: PageItem>(&mut self, item: &dyn PageItem) -> Option<OffsetNumber> {
        let item_id_size = std::mem::size_of::<ItemIdData>() as u16;
        let data_len = item.len() as u16;
        let needed_space = data_len as u16 + item_id_size;
        if self.get_free_space() < needed_space as usize {
            return None;
        }
        let mut header = self.get_header();
        header.pd_upper -= data_len;
        let lp_off = header.pd_upper;
        let lower_ptr_pos = header.pd_lower;
        header.pd_lower += item_id_size;
        self.set_header(&header);

        let item_id = ItemIdData::new(lp_off, data_len as u16, item_id_flags::LP_NORMAL);
        let item_id_bytes = item_id.as_bytes();
        self.data[lower_ptr_pos as usize..(lower_ptr_pos as usize + item_id_size as usize)]
            .copy_from_slice(item_id_bytes);
       
        let target_slice = &mut self.data[lp_off as usize..(lp_off as usize + data_len as usize)];
        item.serialize_into(target_slice);
        
        let header_size = std::mem::size_of::<PageHeaderData>() as u16;
        Some((lower_ptr_pos - header_size) / item_id_size + 1)
    }

    pub(crate) fn get_item(&self, slot_num: OffsetNumber) -> Option<&[u8]> {
        let item_id = self.get_item_id(slot_num)?;
        // Only return a view if the item is marked as normal (not deleted or redirected)
        if item_id.lp_flags() != item_id_flags::LP_NORMAL {
            return None; // TODO: VACCUM may require more freedom.
        }
        let start = item_id.lp_off() as usize;
        let end = start + item_id.lp_len() as usize;
        self.data.get(start..end)
    }
    // pub(crate) fn get_tuple_view(&'_ self, slot_num: OffsetNumber) -> Option<HeapTupleView<'_>> {
    //     if slot_num == 0 {
    //         return None;
    //     }
    //     let header_size = std::mem::size_of::<PageHeaderData>() as u16;
    //     let item_id_size = std::mem::size_of::<ItemIdData>() as u16;
    //     let offset_to_id = header_size + (slot_num - 1) * item_id_size;
    //     if offset_to_id + item_id_size > self.get_header().pd_lower {
    //         return None;
    //     }
    //     let item_id = unsafe {
    //         let ptr = self.data.as_ptr().add(offset_to_id as usize) as *const ItemIdData;
    //         std::ptr::read_unaligned(ptr)
    //     };
    //     // Only return a view if the item is marked as normal (not deleted or redirected)
    //     if item_id.lp_flags() != item_id_flags::LP_NORMAL {
    //         return None;
    //     }
    //     let start = item_id.lp_off() as usize;
    //     let end = start + item_id.lp_len() as usize;
    //     let raw_tuple = &self.data[start..end];
    //     Some(HeapTupleView::new(raw_tuple))
    // }
}


impl Page {
    pub(crate) fn update_item_header<H, F>(&mut self, slot_num: OffsetNumber, f: F) 
        where 
            H: FromBytes + IntoBytes + KnownLayout, 
            F: FnOnce(&mut H)
    {
        let item_id = match self.get_item_id(slot_num) {
            Some(id) if id.lp_flags() == item_id_flags::LP_NORMAL => id,
            _ => return,
        };
        let offset = item_id.lp_off() as usize;
        let header_len = std::mem::size_of::<H>();

        if (item_id.lp_len() as usize) < header_len {
            return;
        }
        if let Some(bytes) = self.data.get_mut(offset..offset + header_len) {
            if let Ok(header_ref) = H::mut_from_bytes(bytes) {
                f(header_ref);
                return;
            }
        }
    }
}
