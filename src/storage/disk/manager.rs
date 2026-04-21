use crate::common::constants::BLCKSZ;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write, Seek, SeekFrom};
use crate::storage::page::page::Page;

pub struct Table { pub oid: u32, file: File }

impl Table {
    // Tables have special_size set to zero, 
    // TODO: we can use special space for FSM or VM in the future
    pub(crate) fn open(oid: u32) -> Self {
        let path = format!("data/{}", oid);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .expect(&format!("ERR: Table {} doesn't exist.", oid));
        
        Self { oid, file }
    }
    pub(crate) fn create(oid: u32, special_size: u16) -> Self {
        let path = format!("data/{}", oid);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true) // error if exists
            .open(&path)
            .expect(&format!("ERR: Table {} already exists.", oid));
        let mut table = Self { oid, file };
        table.extend(special_size);
        table
    }

    pub(crate) fn extend(&mut self, special_size: u16) -> u32 {
        let new_page_id = self.num_pages();
        let empty_page = Page::new(special_size);
        self.write_page(new_page_id, &empty_page);
        new_page_id
    }

    pub(crate) fn read_page(&mut self, page_id: u32) -> Page {
        let mut page = Page::empty();
        self.file.seek(SeekFrom::Start(page_id as u64 * BLCKSZ as u64)).unwrap();
        let _ = self.file.read_exact(&mut page.data);
        page
    }
    pub(crate) fn read_page_raw(&mut self, page_idx: u32) -> [u8; BLCKSZ] {
        let mut buf = [0u8; BLCKSZ];
        let offset = (page_idx as u64) * (BLCKSZ as u64);
        self.file.seek(std::io::SeekFrom::Start(offset)).expect("Seek failed");
        self.file.read_exact(&mut buf).expect("Read page failed");
        buf
    }
    pub(crate) fn write_page(&mut self, page_id: u32, page: &Page) {
        self.file.seek(SeekFrom::Start(page_id as u64 * BLCKSZ as u64)).unwrap();
        self.file.write_all(&page.data).unwrap();
        self.file.flush().unwrap();
    }
    pub(crate) fn write_page_raw(&mut self, page_idx: u32, data: &[u8]) {
        self.file.seek(SeekFrom::Start((page_idx as u64) * (BLCKSZ as u64))).unwrap();
        self.file.write_all(data).unwrap();
        self.file.flush().unwrap();
    }
    pub fn num_pages(&self) -> u32 {
        self.file.metadata().unwrap().len() as u32 / BLCKSZ as u32
    }
}