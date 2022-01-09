use std::io::{Read, Seek, Write};

use anyhow::{anyhow, bail, Context, Result};
use arrow2::array::Array;
use arrow2::datatypes::{DataType, Field, Schema};
use arrow2::io::parquet::read;
use arrow2::io::parquet::write::Encoding;

use crate::table::VarArray;
use crate::{Kind, TableField, Writer};

#[derive(Clone)]
pub struct OutField {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,

    pub encoding: Encoding,
}

// struct Transform {
//     input: String,
//     output: OutField,
//     func: Box<dyn FnMut(Box<dyn Array>, &mut Table, usize) -> Result<()>>,
// }

pub struct Split {
    pub output: Vec<OutField>,
    pub func: Box<dyn Send + FnMut(Box<dyn Array>, &mut [&mut VarArray]) -> Result<()>>,
}

pub enum Action {
    ErrorOut,
    Drop,
    Copy,
    // Transform(Transform),
    Split(Split),
}

pub struct Op {
    pub input: String,
    pub action: Action,
}

pub struct Repack {
    pub ops: Vec<Op>,
}

fn find_field<'f>(schema: &'f Schema, name: &str) -> Option<(usize, &'f Field)> {
    schema
        .fields()
        .iter()
        .enumerate()
        .find(|(_, f)| f.name == name)
}

pub fn transform<W: Write + Send + 'static>(
    mut f: impl Read + Seek,
    out: W,
    repack: &mut Repack,
) -> Result<W> {
    let metadata = read::read_metadata(&mut f)?;
    let in_schema = read::get_schema(&metadata)?;

    let out_schema = repack
        .ops
        .iter()
        .flat_map(|op| -> Vec<Result<OutField>> {
            match &op.action {
                Action::Drop | Action::ErrorOut => Vec::new(),
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

                Action::Split(split) => split.output.iter().cloned().map(Ok).collect(),
            }
        })
        .collect::<Result<Vec<OutField>>>()?;

    let table_schema = out_schema
        .iter()
        .map(|v| -> Result<TableField> {
            Ok(TableField {
                name: v.name.to_string(),
                kind: Kind::from_arrow(&v.data_type)
                    .with_context(|| anyhow!("converting {:?} to a Kind", v.name))?,
                nullable: false,
                encoding: Encoding::Plain,
            })
        })
        .collect::<Result<Vec<_>>>()
        .with_context(|| anyhow!("generating an internal schema for the output"))?;

    let mut writer = Writer::new(out, &table_schema)?;

    for (rg, _rg_meta) in metadata.row_groups.iter().enumerate() {
        for op in &mut repack.ops {
            let (field, field_meta) = find_field(&in_schema, &op.input)
                .ok_or_else(|| anyhow!("looking up input field {:?}", op.input))?;
            let col = read::get_column_iterator(&mut f, &metadata, rg, field, None, Vec::new());
            // these _ are the returned cache buffers
            let (arr, _, _) = read::column_iter_to_array(col, field_meta, Vec::new())?;

            match &mut op.action {
                Action::ErrorOut => bail!("asked to error out after loading {:?}", field_meta.name),
                Action::Drop => unimplemented!("drop"),
                Action::Copy => unimplemented!("copy"),
                Action::Split(s) => {
                    let fields: Vec<usize> = s
                        .output
                        .iter()
                        .map(|f| {
                            writer
                                .find_field(&f.name)
                                .expect("created based on input")
                                .0
                        })
                        .collect();
                    (s.func)(arr, &mut writer.table().get_many(&fields))?;
                }
            }
        }

        writer.finish_row()?;
    }

    writer.finish()
}
