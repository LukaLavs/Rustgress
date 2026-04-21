use crate::access::tuple::desc::{TupleDescriptor};
use crate::access::tuple::tuple::HeapTuple;

pub trait RGSomething: Sized {
    fn get_descriptor() -> TupleDescriptor;
    fn make_tuple(self, schema: &TupleDescriptor) -> HeapTuple;
    fn from_tuple(tuple: &HeapTuple) -> Self;
}
