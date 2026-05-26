use crate::common::constants::BLCKSZ;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write, Seek, SeekFrom};
use crate::storage::page::page::Page;
use crate::utils::debug::errors::DiskError;
pub struct Table { pub oid: u32, file: File }

impl Table {
    // Tables have special_size set to zero, 
    // TODO: we can use special space for FSM or VM in the future
    pub(crate) fn open(oid: u32) -> Result<Self, DiskError> {
        let path = format!("data/{}", oid);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .map_err(|err| DiskError::TableNotFound { 
                oid: oid, 
                path: path.clone(), 
                source: err, 
            })?;
        
        Ok(Self { oid, file })
    }
    pub(crate) fn create(oid: u32, special_size: u16) -> Result<Self, DiskError> {
        let path = format!("data/{}", oid);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true) // error if exists
            .open(&path)
            .map_err(|err| DiskError::TableAlreadyExists { 
                oid: oid, 
                path: path.clone(), 
                source: err, 
            })?;
        let mut table = Self { oid, file };
        table.extend(special_size);
        Ok(table)
    }

    fn page_offset(page_id: u32) -> u64 {
        page_id as u64 * BLCKSZ as u64
    }

    pub(crate) fn extend(&mut self, special_size: u16) -> Result<u32, DiskError> {
        let new_page_id = self.num_pages()?;
        let empty_page = Page::new(special_size);
        self.write_page(new_page_id, &empty_page)?;
        Ok(new_page_id)
    }

    pub(crate) fn read_page(&mut self, page_id: u32) -> Result<Page, DiskError> {
        let mut page = Page::empty();
        let offset = Table::page_offset(page_id);
        self.file.seek(SeekFrom::Start(offset))
            .map_err(|err| DiskError::SeekFailed { page_id, offset, source: err })?;
        self.file.read_exact(&mut page.data)
            .map_err(|err| DiskError::ReadFailed { page_id, source: err})?;
        Ok(page)
    }
    pub(crate) fn read_page_raw(&mut self, page_id: u32) -> Result<[u8; BLCKSZ], DiskError> {
        let mut buf = [0u8; BLCKSZ];
        let offset = Table::page_offset(page_id);
        self.file.seek(std::io::SeekFrom::Start(offset))
            .map_err(|err| DiskError::SeekFailed { page_id, offset, source: err })?;
        self.file.read_exact(&mut buf)
            .map_err(|err| DiskError::ReadFailed { page_id, source: err})?;
        Ok(buf)
    }
    pub(crate) fn write_page(&mut self, page_id: u32, page: &Page) -> Result<(), DiskError>{
        let offset = Table::page_offset(page_id);
        self.file.seek(SeekFrom::Start(offset))
            .map_err(|err| DiskError::SeekFailed { page_id, offset, source: err })?;
        self.file.write_all(&page.data)
            .map_err(|err| DiskError::WriteFailed { page_id, source: err })?;
        self.file.flush()
            .map_err(|err| DiskError::WriteFailed { page_id, source: err })?;
        Ok(())
    }
    pub(crate) fn write_page_raw(&mut self, page_id: u32, data: &[u8]) -> Result<(), DiskError> {
        let offset = Table::page_offset(page_id);
        self.file.seek(SeekFrom::Start(offset))
            .map_err(|err| DiskError::SeekFailed { page_id, offset: offset, source: err })?;
        self.file.write_all(data)
            .map_err(|err| DiskError::WriteFailed { page_id, source: err })?;
        self.file.flush()
            .map_err(|err| DiskError::WriteFailed { page_id, source: err })?;
        Ok(())
    }
    pub fn num_pages(&self) -> Result<u32, DiskError> {
        let metadata = self.file.metadata()
            .map_err(|err| DiskError::MetadataFailed { source: err })?;
        Ok(metadata.len() as u32 / BLCKSZ as u32)
    }
}