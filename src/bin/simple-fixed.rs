use apache_avro::types::{Record, Value};
use apache_avro::Schema;
use apache_avro::Writer;
use std::fs::File;

fn main() -> anyhow::Result<()> {
    let schema_str = r#"
    {
        "name": "record1",
        "type": "record",
        "namespace": "ns1",
        "fields": [
            {
                "name": "f1",
                "type": {
                    "name": "fixed1",
                    "type": "fixed",
                    "size": 5
                }
            },  {
                "name": "f2",
                "type": {
                    "name": "fixed2",
                    "namespace": "ns2",
                    "type": "fixed",
                    "size": 10
                }
            },  {
                "name": "f3",
                "type": {
                    "name": "union1",
                    "type": [
                        "null",
                        {
                            "name": "fixed3",
                            "type": "fixed",
                            "size": 6
                        }
                    ]
                }
            }
        ]
    }"#;

    let schema = Schema::parse_str(schema_str)?;
    let mut writer = Writer::new(
        &schema,
        File::create(format!(
            "{}/data/simple_fixed.avro",
            env!("CARGO_MANIFEST_DIR")
        ))?,
    );

    [
        (b"abcde", b"fghijklmno", Some(b"ABCDEF")),
        (b"12345", b"1234567890", None),
    ]
    .into_iter()
    .try_for_each(|(data1, data2, data3)| -> anyhow::Result<()> {
        let mut record = Record::new(writer.schema()).unwrap();
        record.put("f1", Value::Fixed(data1.len(), data1.to_vec()));
        record.put("f2", Value::Fixed(data2.len(), data2.to_vec()));
        record.put(
            "f3",
            data3
                .map(|v| Value::Union(1, Box::new(Value::Fixed(v.len(), v.to_vec()))))
                .unwrap_or(Value::Union(0, Box::new(Value::Null))),
        );
        writer.append(record)?;
        Ok(())
    })?;
    writer.flush()?;

    Ok(())
}
