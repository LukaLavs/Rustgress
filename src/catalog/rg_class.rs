use zerocopy_derive::{IntoBytes, FromBytes, Immutable, KnownLayout};

#[derive(IntoBytes, FromBytes, Immutable, KnownLayout, Debug)]
#[repr(C)]
pub struct FormData_pg_class {
    pub oid: u32,             // unique table identifier
    pub relname: [u8; 64],    // table name
    pub relnamespace: u32,    // which schema it belongs to
    pub relpages: u32,        // how many pages it occupies (for optimizer)
    pub reltuples: f32,       // approximate number of rows
    // ... perhaps more metadata should be added later
}