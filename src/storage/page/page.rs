use super::header::PageHeaderData;
use super::item::{PageItem, ItemIdData, item_id_flags};
use crate::common::types::{LocationIndex, OffsetNumber};
use crate::common::constants::{BLCKSZ};
use core::option::Option;
use zerocopy::{FromBytes, IntoBytes};
use crate::utils::debug::errors::PageError;
use zerocopy_derive::{KnownLayout, Immutable, FromBytes};

#[repr(align(8))]
#[derive(KnownLayout, Immutable, FromBytes)]
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

    pub(crate) fn get_header(&self) -> Result<PageHeaderData, PageError> {
        PageHeaderData::read_from_prefix(&self.data)
            .map(|(header, _)| header)
            .map_err(|_| PageError::InvalidHeader)
    }

    pub(crate) fn set_header(&mut self, header: &PageHeaderData) {
        let bytes = header.as_bytes();
        self.data[..bytes.len()].copy_from_slice(bytes);
    }

    fn get_free_space(&self) -> Result<usize, PageError> {
        let h = self.get_header()?;
        Ok((h.pd_upper.saturating_sub(h.pd_lower)) as usize)
    }

    pub(crate) fn from_bytes(bytes: &[u8]) -> Result<&Self, PageError> {
        zerocopy::Ref::<&[u8], Page>::from_prefix(bytes)
            .map(|(r, _reminder)| zerocopy::Ref::into_ref(r))
            .map_err(|_| PageError::ConversionFailed)
        // unsafe { &*(bytes.as_ptr() as *const Page) } // this tehnically works, we try to use zerocopy.
    } 
}

impl Page { // Item managment methods.
    fn slot_offset(slot_num: OffsetNumber) -> (usize, usize) {
        let header_size = std::mem::size_of::<PageHeaderData>();
        let item_id_size = std::mem::size_of::<ItemIdData>();
        (header_size + (slot_num as usize - 1) * item_id_size, item_id_size)
    }

    pub(crate) fn get_item_id(&self, slot_num: OffsetNumber) -> Option<ItemIdData> {
        let (offset, item_size) = Page::slot_offset(slot_num);
        let header = self.get_header().ok()?;
        if slot_num == 0 { return None; }
        if offset + item_size > header.pd_lower as usize {
            return None;
        }
        ItemIdData::read_from_prefix(&self.data[offset..])
            .ok().map(|(id, _)| id)
    }
    pub(crate) fn get_item_id_mut(&mut self, slot_num: OffsetNumber) -> Option<&mut ItemIdData> {
        if slot_num == 0 { return None; }
        let (offset, item_size) = Page::slot_offset(slot_num);
        let header = self.get_header().ok()?;
        if offset + item_size > header.pd_lower as usize {
            return None;
        }
        ItemIdData::mut_from_prefix(&mut self.data[offset..])
            .ok()
            .map(|(id, _remainder)| id)
    }

    /// Attempts to add an item to the page. Returns the offset (slot) number to the new item
    /// if successful, or None if there is not enough free space.
    pub(crate) fn add_item<T: PageItem>(&mut self, item: &dyn PageItem) -> Result<OffsetNumber, PageError> {
        let item_id_size = std::mem::size_of::<ItemIdData>() as u16;
        let data_len = item.len() as u16;
        let aligned_len = (data_len + 3) & !3; // must align to at least 4 bytes
        let needed_space = (data_len + item_id_size) as usize;
        if self.get_free_space()? < needed_space {
            return Err(PageError::ItemAddingFailureNotEnoughSpace { item_len: aligned_len as usize, free_space: needed_space });
        }
        let mut header = self.get_header()?;
        let num_slots = header.num_slots();
        let mut target_slot: Option<OffsetNumber> = None;
        for slot in 1..=num_slots {
            if let Some(item_id) = self.get_item_id(slot) {
                if item_id.is_unused() {
                    target_slot = Some(slot);
                    break;
                }
            }
        }
        header.pd_upper -= aligned_len;
        let lp_off = header.pd_upper;

        let slot_num = if let Some(slot_idx) = target_slot {
            if let Some(item_id) = self.get_item_id_mut(slot_idx) {
                *item_id = ItemIdData::new(lp_off, data_len, item_id_flags::LP_NORMAL);
            } else {
                return Err(PageError::ItemAddingFailureNullSlot { slot_num: slot_idx });
            }
            slot_idx
        } else {
            let lower_ptr_pos = header.pd_lower;
            header.pd_lower += item_id_size;
            let item_id = ItemIdData::new(lp_off, data_len, item_id_flags::LP_NORMAL);
            let offset = lower_ptr_pos as usize;
            self.data[offset..offset + item_id_size as usize].copy_from_slice(item_id.as_bytes());
            header.num_slots() 
        };
        self.set_header(&header);
        let target_slice = &mut self.data[lp_off as usize..(lp_off as usize + data_len as usize)];
        item.serialize_into(target_slice);

        Ok(slot_num)
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
    
    pub(crate) fn update_item_header<H, F>(&mut self, slot_num: OffsetNumber, f: F) 
        -> Result<(), PageError>
        where 
            H: zerocopy::FromBytes + zerocopy::IntoBytes + zerocopy::KnownLayout, 
            F: FnOnce(&mut H)
    {
        let item_id = match self.get_item_id(slot_num) {
            Some(id) if id.lp_flags() == item_id_flags::LP_NORMAL => id,
            _ =>  Err(PageError::UpdateItemHeaderNotNormalSlot { slot_num })?,
        };
        let offset = item_id.lp_off() as usize;
        let header_len = std::mem::size_of::<H>();

        if (item_id.lp_len() as usize) < header_len {
            return Err(PageError::UpdateItemHeaderUnexpectedItemSize { slot_num, len: item_id.lp_len() as usize, expected: header_len });
        }
        if let Some(bytes) = self.data.get_mut(offset..offset + header_len) {
            match H::mut_from_bytes(bytes) {
                Ok(header_ref) => {
                    f(header_ref);
                    return Ok(());
                }
                Err(_napaka) => {
                    return Err(PageError::UpdateItemHeaderAlignmentIssue { slot_num, header_len });
                }
            }
        } else {
            return Err(PageError::UpdateItemHeaderOutOfBounds { slot_num, offset, header_len });
        }
    }
}


impl Page { // Cleanup and compaction methods. Used mainly by VACCUM.
    /// Defragmentation, removal of 'dead' tuples, and update of ItemIdData flags are all done in one pass to 
    /// minimize overhead.
    pub(crate) fn compact_page(&mut self) -> Result<(), PageError> {
        let old_header = self.get_header()?;
        let num_slots = old_header.num_slots();
        let mut new_page = Page::new(old_header.pd_special);

        for i in 1..=num_slots {
            if let Some(item_id) = self.get_item_id(i) {
                if item_id.is_normal() {
                    let start = item_id.lp_off() as usize;
                    let end = start + item_id.lp_len() as usize;
                    let tuple_data = &self.data[start..end];
                    new_page.add_item::<&[u8]>(&tuple_data)?;
                } else {
                    new_page.add_empty_slot()?; // we must keep the same slot numbers to maintain RowIds.
                }
            }
        }
        self.data.copy_from_slice(&new_page.data);
        Ok(())
    }

    fn add_empty_slot(&mut self) -> Result<(), PageError> {
        let mut header = self.get_header()?;
        let item_id_size = std::mem::size_of::<ItemIdData>() as u16;
        let lower_ptr_pos = header.pd_lower;
        header.pd_lower += item_id_size;
        self.set_header(&header);
        let empty_id = ItemIdData::new(0, 0, item_id_flags::LP_UNUSED);
        self.data[lower_ptr_pos as usize..(lower_ptr_pos as usize + item_id_size as usize)]
            .copy_from_slice(empty_id.as_bytes());
        Ok(())
    }
    // Truncate unused slots at the end of the page to free up space. This should be 
    // called after compact_page to maximize effectiveness.
    pub fn truncate_items(&mut self) -> Result<(), PageError> {
        let mut header = self.get_header()?;
        let item_id_size = std::mem::size_of::<ItemIdData>() as u16;
        let mut current_num_slots = header.num_slots();
        while current_num_slots > 0 {
            if let Some(id) = self.get_item_id(current_num_slots) {
                if id.is_unused() {
                    header.pd_lower -= item_id_size;
                    current_num_slots -= 1;
                } else {
                    break;
                }
            } else {
                break;
            }
        }
        self.set_header(&header);
        Ok(())
    }
}
