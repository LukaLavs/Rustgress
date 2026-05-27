use crate::common::types::{HeapTupleData};
use zerocopy::{IntoBytes, FromBytes};
use super::header::{HeapTupleHeaderData, TupleInfoMask};
use crate::storage::page::item::PageItem;

#[derive(Debug, Clone)]
pub struct HeapTuple {
    pub header: HeapTupleHeaderData,
    pub null_bitmap: Vec<u8>,
    pub data: HeapTupleData,
}

impl HeapTuple {
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

impl PageItem for HeapTuple {
    fn len(&self) -> usize {
        self.header.t_hoff as usize + self.data.len()
    }

    fn serialize_into(&self, dest: &mut [u8]) {
        self.serialize_into(dest);
    }
}

pub struct HeapTupleView<'a> {
    pub header: HeapTupleHeaderData,
    pub null_bitmap: &'a [u8],
    pub data: &'a [u8],
}

impl<'a> HeapTupleView<'a> {
    pub fn new(raw_bytes: &'a [u8]) -> Self {
        let (header, _) = HeapTupleHeaderData::read_from_prefix(raw_bytes)
            .expect("Raw bytes too small for header");
        let header_size = std::mem::size_of::<HeapTupleHeaderData>();
        let hoff = header.t_hoff as usize;
        let mask = header.read_infomask();
        let null_bitmap = if mask.contains(TupleInfoMask::HEAP_HASNULL) && hoff > header_size {
            &raw_bytes[header_size..hoff]
        } else {
            &raw_bytes[..0] // empty slice if no null bitmap
        };
        let data = &raw_bytes[hoff..];
        HeapTupleView {
            header,
            null_bitmap,
            data,
        }
    }
    pub fn data(&self) -> &[u8] { self.data }
    pub fn null_bitmap(&self) -> Option<&[u8]> {
        if self.null_bitmap.is_empty() { None } else { Some(self.null_bitmap) }
    }
    pub fn to_tuple(&self) -> HeapTuple {
        HeapTuple {
            header: self.header,
            null_bitmap: self.null_bitmap.to_vec(),
            data: self.data.to_vec(),
        }
    }
}
