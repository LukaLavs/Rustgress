use thiserror::Error;


#[derive(Error, Debug)]
pub enum DiskError {
    #[error("Concurrency error: {0}")]
    Lock(#[from] LockError),

    #[error("Table {oid} doesn't exist at path '{path}'. (Original OS error: {source})")]
    TableNotFound {
        oid: u32,
        path: String,
        #[source] source: std::io::Error,
    },
    #[error("Table {oid} already exists at path '{path}'. (Original OS error: {source})")]
    TableAlreadyExists {
        oid: u32,
        path: String,
        #[source] source: std::io::Error,
    },
    #[error("Failed to write page {page_id} to disk. (OS error: {source})")]
    WriteFailed { page_id: u32, #[source] source: std::io::Error },

    #[error("Failed to read page {page_id} from disk. (OS error: {source})")]
    ReadFailed { page_id: u32, #[source] source: std::io::Error },

    #[error("Failed to seek to page {page_id} (offset: {offset}). (OS error: {source})")]
    SeekFailed { page_id: u32, offset: u64, #[source] source: std::io::Error },

    #[error("Failed to read metadata for table. (OS error: {source})")]
    MetadataFailed { #[source] source: std::io::Error, },
}

#[derive(Error, Debug)]
pub enum PageError {
    #[error("Page data is too small or corrupted to contain a valid header.")]
    InvalidHeader,

    #[error("Memory alignment or bounds check failed during conversion.")]
    ConversionFailed,

    #[error("Can not add item of length {item_len} to page with only {free_space} bytes of free space.")]
    ItemAddingFailureNotEnoughSpace { item_len: usize, free_space: usize },

    #[error("Can not add item to non existent slot.")]
    ItemAddingFailureNullSlot { slot_num: u16 },

    #[error("Tried updating header for non-normal slot {slot_num}.")]
    UpdateItemHeaderNotNormalSlot { slot_num: u16 },

    #[error("Item in slot {slot_num} has length {len} which is smaller than expected header length {expected}!")]
    UpdateItemHeaderUnexpectedItemSize { slot_num: u16, len: usize, expected: usize },

    #[error("Alignment issue slot {slot_num} with expected header length {header_len}.")]
    UpdateItemHeaderAlignmentIssue { slot_num: u16, header_len: usize },

    #[error("Out of bounds access for slot {slot_num} with offset {offset} and header length {header_len}.")]
    UpdateItemHeaderOutOfBounds { slot_num: u16, offset: usize, header_len: usize },
}

#[derive(Error, Debug, Clone, Copy)]
#[error("Internal concurrency error: a lock was poisoned.")]
pub struct LockError;

#[derive(Error, Debug)]
pub enum BufferPoolError {
    #[error(transparent)]
    Disk(#[from] DiskError),

    #[error(transparent)]
    Page(#[from] PageError),

    #[error("Concurrency error: {0}")]
    Lock(#[from] LockError),

    #[error("Data corruption detected! Checksum verification failed for table {table_oid}, page {page_idx}.")]
    DataCorrupted { table_oid: u32, page_idx: u32 },

    #[error("Checksum verification failed for page {page_id} of table {table_oid}.")]
    ChecksumFailed { page_id: u32, table_oid: u32 },
}

#[derive(Error, Debug)]
pub enum AccessError {
    #[error(transparent)]
    Disk(#[from] DiskError),

    #[error(transparent)]
    Page(#[from] PageError),

    #[error("Buffer pool error: {0}")]
    BufferPool(#[from] BufferPoolError),

    #[error("Concurrency error: {0}")]
    Lock(#[from] LockError),

    #[error("No active transaction found in context for operation that requires an active transaction.")]
    NoActiveTransactions,

    #[error("Table '{0}' not found in catalog.")]
    TableNotFound(String),

    #[error("Failed to create data folder for system catalogs.")]
    DataFolderCreationFailed,

    #[error("Duplicated table names")]
    DuplicatedTableNames,
}


#[derive(Error, Debug)]
pub enum RustgressError {
    #[error("Access layer error: {0}")]
    Access(#[from] AccessError),

    #[error("Disk subsystem error: {0}")]
    Disk(#[from] DiskError),

    #[error("Page structure error: {0}")]
    Page(#[from] PageError),

    #[error("Buffer pool error: {0}")]
    BufferPool(#[from] BufferPoolError),

    #[error("Concurrency lock error: {0}")]
    Lock(#[from] LockError),

    #[error("SQL Parser error: {0}")]
    Parser(String),

    #[error("Execution engine error: {0}")]
    Execution(String),

    #[error("Transaction control error: {0}")]
    Transaction(String),
}

impl From<String> for RustgressError {
    fn from(err: String) -> Self {
        RustgressError::Parser(err)
    }
}

impl From<&str> for RustgressError {
    fn from(err: &str) -> Self {
        RustgressError::Parser(err.to_string())
    }
}