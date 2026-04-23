use crate::storage::page::page::Page;
use crate::storage::page::header::PageHeaderData;

pub trait PageChecksumExt {
    fn compute_checksum(&self) -> u16;
    fn checksum_verified(&self) -> bool;
    fn update_checksum(&mut self);
}

impl PageChecksumExt for Page {
    fn compute_checksum(&self) -> u16 {
        // Offset of pd_checksum within PageHeaderData
        // Safe because it doesn't create a reference to unaligned data
        let offset = offset_of_checksum();
        
        // Copy 8KB page to stack for calculation
        let mut temp_data = self.data; 
        
        // Zero out the checksum field before hashing
        temp_data[offset] = 0;
        temp_data[offset + 1] = 0;

        // Calculate CRC32
        let crc = crc32fast::hash(&temp_data);

        // Fold 32-bit CRC into 16-bit to fit pd_checksum field
        ((crc >> 16) ^ (crc & 0xFFFF)) as u16
    }

    fn checksum_verified(&self) -> bool {
        let header = self.get_header();
        // 0 is often used to indicate checksums are disabled/unset
        if header.pd_checksum == 0 {
            return true;
        }
        header.pd_checksum == self.compute_checksum()
    }

    fn update_checksum(&mut self) {
        let new_checksum = self.compute_checksum();
        let mut header = self.get_header();
        header.pd_checksum = new_checksum;
        self.set_header(&header);
    }
}

/// Safely calculates the offset of pd_checksum using addr_of!
/// This avoids Undefined Behavior by not creating intermediate references.
fn offset_of_checksum() -> usize {
    unsafe {
        let base = std::ptr::null::<PageHeaderData>();
        // Using addr_of! is the officially sanctioned way to get offsets 
        // in packed/unaligned structs.
        std::ptr::addr_of!((*base).pd_checksum) as usize
    }
}