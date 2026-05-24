use std::cell::Cell;
use crate::common::types::TransactionId;

thread_local! {
    // Only one thread accesses this at a time. 0 is for InvalidTransactionId, 
    // meaning no active transaction or a regular scan.
    static CURRENT_XID: Cell<TransactionId> = Cell::new(0);
}

pub fn set_current_xid(xid: TransactionId) {
    CURRENT_XID.with(|ctx| ctx.set(xid));
}

pub fn get_current_xid() -> TransactionId {
    CURRENT_XID.with(|ctx| ctx.get())
}

pub fn clear_current_xid() {
    CURRENT_XID.with(|ctx| ctx.set(0));
}