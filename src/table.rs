use std::any::Any;
use std::sync::Arc;

use crate::MemUsage;
use anyhow::{anyhow, bail, ensure, Result};
use arrow2::array::{
    Array, MutableArray, MutableBooleanArray, MutableFixedSizeBinaryArray, MutablePrimitiveArray,
    MutableUtf8Array, TryPush,
};
use arrow2::datatypes::{DataType, TimeUnit};
use arrow2::io::parquet::write::Encoding;
use arrow2::types::NativeType;

#[derive(Clone)]
pub struct TableField {
    pub name: String,
    pub kind: Kind,
    pub nullable: bool,

    pub encoding: Encoding,
}

impl TableField {
    pub fn new(name: impl ToString, kind: Kind, nullable: bool) -> Self {
        TableField {
            name: name.to_string(),
            kind,
            nullable,
            encoding: kind.default_encoding(),
        }
    }
}

#[derive(Copy, Clone)]
pub enum Kind {
    Bool,
    Uuid,
    U8,
    I32,
    I64,
    F64,
    String,

    // do we want multiple types here?
    TimestampSecsZ,
}

impl Kind {
    pub fn array_with_capacity(self, capacity: usize) -> VarArray {
        match self {
            Kind::Bool => VarArray::new(MutableBooleanArray::with_capacity(capacity)),
            Kind::U8 => VarArray::new(MutablePrimitiveArray::<u8>::with_capacity(capacity)),
            Kind::I32 => VarArray::new(MutablePrimitiveArray::<i32>::with_capacity(capacity)),
            Kind::I64 => VarArray::new(MutablePrimitiveArray::<i64>::with_capacity(capacity)),
            Kind::F64 => VarArray::new(MutablePrimitiveArray::<f64>::with_capacity(capacity)),
            Kind::String => VarArray::new(MutableUtf8Array::<i32>::with_capacity(capacity)),
            Kind::Uuid => VarArray::new(MutableFixedSizeBinaryArray::with_capacity(16, capacity)),
            Kind::TimestampSecsZ => {
                VarArray::new(MutablePrimitiveArray::<i64>::with_capacity(capacity))
            }
        }
    }

    pub fn to_arrow(self) -> DataType {
        match self {
            Kind::Bool => DataType::Boolean,
            Kind::U8 => DataType::UInt8,
            Kind::I32 => DataType::Int32,
            Kind::I64 => DataType::Int64,
            Kind::F64 => DataType::Float64,
            Kind::String => DataType::Utf8,
            Kind::Uuid => DataType::FixedSizeBinary(16),
            Kind::TimestampSecsZ => DataType::Timestamp(TimeUnit::Second, None),
        }
    }

    pub fn from_arrow(arrow: &DataType) -> Result<Self> {
        Ok(match arrow {
            DataType::Utf8 => Kind::String,
            DataType::Boolean => Kind::Bool,
            DataType::Int64 => Kind::I64,
            DataType::Int32 => Kind::I32,
            DataType::UInt8 => Kind::U8,
            DataType::Float64 => Kind::F64,
            DataType::Timestamp(TimeUnit::Second, None) => Kind::TimestampSecsZ,
            other => bail!("unsupported type {:?}", other),
        })
    }

    pub fn default_encoding(&self) -> Encoding {
        match self {
            Kind::F64 => Encoding::ByteStreamSplit,
            // don't think there's a reasonable encoding for these
            Kind::Bool | Kind::U8 => Encoding::Plain,
            // maybe this would practically benefit from the string encoding?
            Kind::Uuid => Encoding::Plain,
            // TODO: (writing with arrow2) > External format error: Invalid argument error: The datatype Int32 cannot be encoded by DeltaBinaryPacked
            Kind::TimestampSecsZ | Kind::I64 | Kind::I32 => Encoding::Plain,
            // TODO: (reading with datafusion) > ArrowError(ParquetError("Error reading batch from projects.parquet (size: 286037286): Parquet argument error: NYI: Encoding DELTA_LENGTH_BYTE_ARRAY is not supported"))
            Kind::String => Encoding::Plain,
        }
    }
}

pub struct VarArray {
    pub inner: Box<dyn MutableArray>,
}

impl VarArray {
    fn new<T: MutableArray + 'static>(array: T) -> Self {
        Self {
            inner: Box::new(array),
        }
    }

    pub fn downcast_ref<T: Any>(&self) -> Option<&T> {
        self.inner.as_any().downcast_ref()
    }

    pub fn downcast_mut<T: Any>(&mut self) -> Option<&mut T> {
        self.inner.as_mut_any().downcast_mut()
    }

    // this moves, but has to be called from a mut ref?!
    fn as_arc(&mut self) -> Arc<dyn Array> {
        self.inner.as_arc()
    }
}

impl MemUsage for VarArray {
    fn mem_usage(&self) -> usize {
        // some regrets
        if let Some(v) = self.downcast_ref::<MutableUtf8Array<i32>>() {
            v.mem_usage()
        } else if let Some(v) = self.downcast_ref::<MutablePrimitiveArray<i64>>() {
            v.mem_usage()
        } else if let Some(v) = self.downcast_ref::<MutablePrimitiveArray<i32>>() {
            v.mem_usage()
        } else if let Some(v) = self.downcast_ref::<MutablePrimitiveArray<i16>>() {
            v.mem_usage()
        } else if let Some(v) = self.downcast_ref::<MutablePrimitiveArray<u8>>() {
            v.mem_usage()
        } else if let Some(v) = self.downcast_ref::<MutableBooleanArray>() {
            v.mem_usage()
        } else {
            debug_assert!(false, "unsupported type");
            // just wildly overestimate
            self.inner.len() * 16
        }
    }
}

pub struct Table {
    schema: Box<[Kind]>,
    builders: Box<[VarArray]>,
    cap: usize,
    mem_used: usize,
}

fn make_builders(schema: &[Kind], cap: usize) -> Box<[VarArray]> {
    schema
        .iter()
        .map(|kind| kind.array_with_capacity(cap))
        .collect()
}

impl Table {
    pub fn with_capacity(schema: &[Kind], cap: usize) -> Self {
        Self {
            schema: schema.to_vec().into_boxed_slice(),
            builders: make_builders(schema, cap),
            cap,
            mem_used: 0,
        }
    }

    pub fn check_consistent(&self) -> Result<()> {
        let expectation = self.builders[0].inner.len();
        for (i, b) in self.builders.iter().enumerate().skip(1) {
            ensure!(
                b.inner.len() == expectation,
                "expected col {} to have length {}, not {}",
                i,
                expectation,
                b.inner.len()
            );
        }

        Ok(())
    }

    pub fn mem_estimate(&self) -> usize {
        self.mem_used
    }

    pub fn get(&mut self, item: usize) -> &mut VarArray {
        &mut self.builders[item]
    }

    pub fn get_many(&mut self, items: &[usize]) -> Vec<&mut VarArray> {
        self.builders
            .iter_mut()
            .enumerate()
            .filter(|(i, _)| items.contains(i))
            .map(|(_, v)| v)
            .collect()
    }

    pub fn finish_bulk_push(&mut self) -> Result<()> {
        self.check_consistent()?;
        self.mem_used = self.builders.iter().map(|b| b.mem_usage()).sum();
        Ok(())
    }

    pub fn rows(&self) -> usize {
        self.builders[0].inner.len()
    }

    pub fn push_null(&mut self, i: usize) -> Result<()> {
        // only off by a factor of about eight
        self.mem_used += 1;
        self.builders[i].inner.push_null();
        Ok(())
    }

    pub fn push_str(&mut self, i: usize, val: Option<&str>) -> Result<()> {
        let arr = &mut self.builders[i];
        if let Some(arr) = arr.downcast_mut::<MutableUtf8Array<i32>>() {
            self.mem_used +=
                val.map(|val| val.len()).unwrap_or_default() + std::mem::size_of::<i32>();
            arr.try_push(val)?;
            Ok(())
        } else {
            Err(anyhow!("can't push a string to this column"))
        }
    }

    pub fn push_bool(&mut self, i: usize, val: Option<bool>) -> Result<()> {
        let arr = &mut self.builders[i];
        if let Some(arr) = arr.downcast_mut::<MutableBooleanArray>() {
            // only off by a factor of about four
            self.mem_used += 1;
            arr.try_push(val)?;
            Ok(())
        } else {
            Err(anyhow!("can't push a bool to this column"))
        }
    }

    pub fn push_fsb(&mut self, i: usize, val: Option<impl AsRef<[u8]>>) -> Result<()> {
        let arr = &mut self.builders[i];
        let val = match val {
            Some(val) => val,
            None => {
                arr.inner.push_null();
                return Ok(());
            }
        };

        if let Some(arr) = arr.downcast_mut::<MutableFixedSizeBinaryArray>() {
            self.mem_used += arr.size();
            arr.try_push(Some(val.as_ref()))?;
            Ok(())
        } else {
            Err(anyhow!("can't push a uuid to this column"))
        }
    }

    pub fn push_primitive<T: NativeType>(&mut self, i: usize, val: Option<T>) -> Result<()> {
        let arr = &mut self.builders[i];
        if let Some(arr) = arr.downcast_mut::<MutablePrimitiveArray<T>>() {
            self.mem_used += std::mem::size_of::<T>();
            arr.try_push(val)?;
            Ok(())
        } else {
            Err(anyhow!(
                "can't push an {} to this column",
                std::any::type_name::<T>()
            ))
        }
    }

    pub fn take_batch(&mut self) -> Vec<Arc<dyn Array>> {
        let ret = self.builders.iter_mut().map(|arr| arr.as_arc()).collect();
        self.builders = make_builders(&self.schema, self.cap);
        self.mem_used = 0;
        ret
    }
}
