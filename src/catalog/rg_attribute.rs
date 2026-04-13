use zerocopy_derive::{IntoBytes, FromBytes, Immutable, KnownLayout};

#[derive(IntoBytes, FromBytes, Immutable, KnownLayout, Debug)]
#[repr(C)]
pub struct FormData_pg_attribute {
    pub attrelid: u32,        // table OID this column belongs to
    pub attname: [u8; 64],    // row name
    pub atttypid: u32,        // type ID of the column
    pub attnum: i16,          // consecutive number of the column in the table
    pub attlen: i16,          // len of column data type (-1 for varlena types)
    // ...
}