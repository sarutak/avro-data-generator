use apache_avro::types::{Record, Value};
use apache_avro::Schema;
use apache_avro::Writer;
use std::fs::File;

fn main() -> anyhow::Result<()> {
    let schema_str = r#"
    {
        "name": "record1",
        "namespace": "ns1",
        "type": "record",
        "fields": [
            {
                "name": "f1",
                "type": {
                    "name": "enum1",
                    "type": "enum",
                    "symbols": ["a", "b", "c", "d"],
                    "default": "a"
                }
            },  {
                "name": "f2",
                "type": {
                    "name": "enum2",
                    "namespace": "ns2",
                    "type": "enum",
                    "symbols": ["e", "f", "g", "h"],
                    "default": "e"
                }
            },  {
                "name": "f3",
                "type": {
                    "name": "union1",
                    "type": [
                        "null",
                        {
                            "name": "enum3",
                            "type": "enum",
                            "symbols": ["i", "j", "k"],
                            "default": "i"
                        }
                    ]
                }
            }
        ]
    }
    "#;

    let schema = Schema::parse_str(schema_str)?;
    let mut writer = Writer::new(
        &schema,
        File::create(format!(
            "{}/data/simple_enum.avro",
            env!("CARGO_MANIFEST_DIR")
        ))?,
    );

    [
        ((0, "a"), (2, "g"), Some((1, "j"))),
        ((1, "b"), (3, "h"), Some((2, "k"))),
        ((2, "c"), (0, "e"), None),
        ((3, "d"), (1, "f"), Some((0, "i"))),
    ]
    .into_iter()
    .try_for_each(|((idx1, sym1), (idx2, sym2), opt)| -> anyhow::Result<()> {
        let mut record = Record::new(writer.schema()).unwrap();
        record.put("f1", Value::Enum(idx1, sym1.into()));
        record.put("f2", Value::Enum(idx2, sym2.into()));
        record.put(
            "f3",
            opt.map(|v| Value::Union(1, Box::new(Value::Enum(v.0, v.1.into()))))
                .unwrap_or(Value::Union(0, Box::new(Value::Null))),
        );
        writer.append(record)?;
        Ok(())
    })?;
    writer.flush()?;

    Ok(())
}
