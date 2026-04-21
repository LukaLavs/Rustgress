pub trait TypeDescriptor {
    type Native: Clone + std::fmt::Debug + PartialEq;
    const OID: u32;
    const NAME: &'static str;
    const IS_FIXED: bool;
    const BYTE_LEN: i32;

    fn pack(val: &Self::Native, buffer: &mut Vec<u8>);
    fn unpack(data: &[u8], cursor: &mut usize) -> Self::Native;
    fn to_string(val: &Self::Native) -> String {
        format!("{:?}", val)
    }
}