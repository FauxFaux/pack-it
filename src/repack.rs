use std::fs;

use anyhow::{anyhow, Result};
use arrow2::array::{Array, MutableArray, MutableBooleanArray};
use arrow2::datatypes::{DataType, Field, Schema};
use arrow2::io::parquet::read;
use arrow2::io::parquet::read::RecordReader;
use arrow2::io::parquet::write::Encoding;

use crate::{Kind, TableField, Writer};

#[derive(Clone)]
pub struct OutField {
    name: String,
    data_type: DataType,
    nullable: bool,

    encoding: Encoding,
}

// struct Transform {
//     input: String,
//     output: OutField,
//     func: Box<dyn FnMut(Box<dyn Array>, Box<dyn MutableArray>) -> Result<()>>,
// }

pub struct Split {
    input: String,
    output: Vec<OutField>,
    func: Box<dyn Send + FnMut(Box<dyn Array>, Vec<Box<dyn MutableArray>>) -> Result<()>>,
}

pub enum Action {
    ErrorOut,
    Copy,
    // Transform(Transform),
    Split(Split),
}

pub struct Op {
    input: String,
    action: Action,
}

pub struct Repack {
    ops: Vec<Op>,
}

fn find_field<'f>(schema: &'f Schema, name: &str) -> Option<(usize, &'f Field)> {
    schema
        .fields()
        .iter()
        .enumerate()
        .find(|(_, f)| f.name == name)
}

pub fn transform(repack: &mut Repack) -> Result<()> {
    let mut f = fs::File::open("1.parquet")?;
    let metadata = read::read_metadata(&mut f)?;
    let in_schema = read::get_schema(&metadata)?;

    let out_schema = repack
        .ops
        .iter()
        .flat_map(|op| -> Vec<Result<OutField>> {
            match &op.action {
                Action::ErrorOut => Vec::new(),
                Action::Copy => vec![
                    try {
                        let x = in_schema.field_with_name(&op.input)?;
                        OutField {
                            name: x.name.to_string(),
                            data_type: x.data_type.clone(),
                            nullable: x.nullable,
                            encoding: Encoding::Plain,
                        }
                    },
                ],

                Action::Split(split) => split.output.iter().cloned().map(|f| Ok(f)).collect(),
            }
        })
        .collect::<Result<Vec<OutField>>>()?;

    let table_schema = out_schema
        .iter()
        .map(|v| -> Result<TableField> {
            let kind = match &v.data_type {
                DataType::Utf8 => Kind::String,
                DataType::Boolean => Kind::Bool,
                DataType::Int64 => Kind::I64,
                DataType::Int32 => Kind::I32,
                DataType::Float64 => Kind::F64,
                other => unimplemented!("table mapping for {:?}", other),
            };
            Ok(TableField {
                name: v.name.to_string(),
                kind,
                nullable: false,
                encoding: Encoding::Plain,
            })
        })
        .collect::<Result<Vec<_>>>()?;

    let writer = Writer::new(fs::File::create("a.parquet")?, &table_schema)?;

    let mut page_buffer = Vec::new();
    let mut buffer = Vec::new();
    for (rg, _rg_meta) in metadata.row_groups.iter().enumerate() {
        for op in &repack.ops {
            let (field, field_meta) = find_field(&in_schema, &op.input)
                .ok_or_else(|| anyhow!("looking up input field {:?}", op.input))?;
            let col = read::get_column_iterator(&mut f, &metadata, rg, field, None, page_buffer);
            let (arr, b1, b2) = read::column_iter_to_array(col, field_meta, buffer)?;
            buffer = b2;
            page_buffer = b1;
        }
    }

    unimplemented!()
}
