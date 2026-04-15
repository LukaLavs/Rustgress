use crate::common::types::{
    OffsetNumber, BLCKSZ,
};
use std::fs::{File, OpenOptions};
use std::io::{Read, Write, Seek, SeekFrom};
use crate::storage::page::layout::Page;
use crate::storage::page::header::PageHeaderData;
use crate::access::tuple::header::{Tuple, ItemIdData, item_id_flags};
use std::ptr;

pub struct Table { pub oid: u32, file: File }

impl Table {
    // Tables have special_size set to zero, 
    // TODO: we can use special space for FSM or VM in the future
    pub fn open(oid: u32) -> Self {
        let path = format!("data/{}", oid);
        let file = OpenOptions::new().read(true).write(true).create(true).open(&path).expect(&format!("Failed to open table file: {}", path));
        Self { oid, file }
    }
    pub fn read_page(&mut self, page_id: u32) -> Page {
        let mut page = Page::empty();
        self.file.seek(SeekFrom::Start(page_id as u64 * BLCKSZ as u64)).unwrap();
        let _ = self.file.read_exact(&mut page.data);
        page
    }
    pub fn read_page_raw(&mut self, page_idx: u32) -> [u8; BLCKSZ] {
        let mut buf = [0u8; BLCKSZ];
        let offset = (page_idx as u64) * (BLCKSZ as u64);
        self.file.seek(std::io::SeekFrom::Start(offset)).expect("Seek failed");
        self.file.read_exact(&mut buf).expect("Read page failed");
        buf
    }
    pub fn write_page(&mut self, page_id: u32, page: &Page) {
        self.file.seek(SeekFrom::Start(page_id as u64 * BLCKSZ as u64)).unwrap();
        self.file.write_all(&page.data).unwrap();
    }
    pub fn write_page_raw(&mut self, page_idx: u32, data: &[u8]) {
        self.file.seek(SeekFrom::Start((page_idx as u64) * (BLCKSZ as u64))).unwrap();
        self.file.write_all(data).unwrap();
        self.file.flush().unwrap();
    }
    pub fn num_pages(&self) -> u32 {
        self.file.metadata().unwrap().len() as u32 / BLCKSZ as u32
    }
    pub fn insert_tuple(&mut self, tuple: &Tuple) {
        let mut last_page_id = self.num_pages().saturating_sub(1);
        let mut page = if self.num_pages() == 0 { last_page_id = 0; Page::new(0) } else { self.read_page(last_page_id) };
        if page.add_tuple(tuple).is_some() {
            self.write_page(last_page_id, &page);
        } else {
            let mut new_page = Page::new(0);
            new_page.add_tuple(tuple);
            self.write_page(last_page_id + 1, &new_page);
        }
    }
    pub fn delete_tuple(&mut self, page_id: u32, slot_num: OffsetNumber) {
        let mut page = self.read_page(page_id);
        
        let header_size = std::mem::size_of::<PageHeaderData>() as u16;
        let item_id_size = std::mem::size_of::<ItemIdData>() as u16;
        let offset_to_id = header_size + (slot_num - 1) * item_id_size;

        if offset_to_id + item_id_size <= page.get_header().pd_lower {
            unsafe {
                let item_id_ptr = page.data.as_mut_ptr().add(offset_to_id as usize) as *mut ItemIdData;
                let mut item_id = ptr::read_unaligned(item_id_ptr);

                item_id.set_lp_flags(item_id_flags::LP_DEAD);
                
                ptr::write_unaligned(item_id_ptr, item_id);
            }
            self.write_page(page_id, &page);
        }
    }
    pub fn update_tuple(&mut self, page_id: u32, slot_num: OffsetNumber, new_tuple: &Tuple) {
        self.delete_tuple(page_id, slot_num); // delete old tuple (mark as dead)

        self.insert_tuple(new_tuple); // insert new tuple
    }
}