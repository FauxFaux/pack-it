use std::fs;

use anyhow::{anyhow, bail, Result};
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
    func: Box<dyn Send + FnMut(Box<dyn Array>, &mut [Box<dyn MutableArray>]) -> Result<()>>,
}

pub enum Action {
    ErrorOut,
    Drop,
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

                Action::Split(split) => split.output.iter().cloned().map(|f| Ok(f)).collect(),
            }
        })
        .collect::<Result<Vec<OutField>>>()?;

    let table_schema = out_schema
        .iter()
        .map(|v| -> Result<TableField> {
            Ok(TableField {
                name: v.name.to_string(),
                kind: Kind::from_arrow(&v.data_type)?,
                nullable: false,
                encoding: Encoding::Plain,
            })
        })
        .collect::<Result<Vec<_>>>()?;

    let writer = Writer::new(fs::File::create("a.parquet")?, &table_schema)?;

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
                    let mut arrs = s
                        .output
                        .iter()
                        .map(|f| Kind::from_arrow(&f.data_type).expect("already checked"))
                        .map(|v| {
                            v.array_with_capacity(
                                usize::try_from(_rg_meta.num_rows())
                                    .expect("TODO: this can actually fail"),
                            )
                            .inner
                        })
                        .collect::<Vec<Box<dyn MutableArray>>>();
                    (s.func)(arr, &mut arrs)?;
                }
            }
        }
    }

    unimplemented!()
}
