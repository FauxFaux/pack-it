use arrow2::array::{
    MutableArray, MutableBooleanArray, MutableFixedSizeBinaryArray, MutablePrimitiveArray,
    MutableUtf8Array, Offset,
};
use arrow2::bitmap::MutableBitmap;
use arrow2::types::NativeType;
use std::mem;

pub trait MemUsage {
    fn mem_usage(&self) -> usize;
}

impl<T: NativeType> MemUsage for Vec<T> {
    #[inline]
    fn mem_usage(&self) -> usize {
        self.len() * mem::size_of::<T>()
    }
}

impl MemUsage for MutableBitmap {
    fn mem_usage(&self) -> usize {
        self.len() / 8
    }
}

impl MemUsage for Option<&MutableBitmap> {
    fn mem_usage(&self) -> usize {
        self.map(|v| v.mem_usage()).unwrap_or(0)
    }
}

impl MemUsage for MutableFixedSizeBinaryArray {
    fn mem_usage(&self) -> usize {
        self.values().mem_usage()
    }
}

impl<O: Offset> MemUsage for MutableUtf8Array<O> {
    fn mem_usage(&self) -> usize {
        self.validity().mem_usage() + self.values().mem_usage() + self.offsets().mem_usage()
    }
}

impl<T: NativeType> MemUsage for MutablePrimitiveArray<T> {
    fn mem_usage(&self) -> usize {
        self.validity().mem_usage() + self.values().mem_usage()
    }
}

impl MemUsage for MutableBooleanArray {
    fn mem_usage(&self) -> usize {
        self.validity().mem_usage() + self.values().mem_usage()
    }
}
