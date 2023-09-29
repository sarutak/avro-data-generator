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
                    "name": "record2",
                    "namespace": "ns2",
                    "type": "record",
                    "fields": [
                        {
                            "name": "f1_1",
                            "type": "string"
                        },  {
                            "name": "f1_2",
                            "type": "int"
                        },  {
                            "name": "f1_3",
                            "type": {
                                "name": "record3",
                                "namespace": "ns3",
                                "type": "record",
                                "fields": [
                                    {
                                        "name": "f1_3_1",
                                        "type": "double"
                                    }
                                ]
                            }
                        }
                    ]
                }
            },  {
                "name": "f2",
                "type": "array",
                "items": {
                    "name": "record4",
                    "namespace": "ns4",
                    "type": "record",
                    "fields": [
                        {
                            "name": "f2_1",
                            "type": "boolean"
                        },  {
                            "name": "f2_2",
                            "type": "float"
                        }
                    ]
                }
            },  {
                "name": "f3",
                "type": [
                    "null",
                    {
                        "name": "record5",
                        "namespace": "ns5",
                        "type": "record",
                        "fields": [
                            {
                                "name": "f3_1",
                                "type": "string"
                            }
                        ]
                    }
                ],
                "default": null
            },  {
                "name": "f4",
                "type": "array",
                "items": [
                    "null",
                    {
                        "name": "record6",
                        "namespace": "ns6",
                        "type": "record",
                        "fields": [
                            {
                                "name": "f4_1",
                                "type": "long"
                            }
                        ]
                    }
                ]
            }
        ]
    }
    "#;

    let schema = Schema::parse_str(schema_str)?;
    let mut writer = Writer::new(
        &schema,
        File::create(format!(
            "{}/data/nested_records.avro",
            env!("CARGO_MANIFEST_DIR")
        ))?,
    );

    let mut record = Record::new(writer.schema()).unwrap();
    record.put(
        "f1",
        Value::Record(vec![
            ("f1_1".into(), "aaa".into()),
            ("f1_2".into(), 10.into()),
            (
                "f1_3".into(),
                Value::Record(vec![("f1_3_1".into(), (3.140).into())]),
            ),
        ]),
    );
    record.put(
        "f2",
        Value::Array(vec![
            Value::Record(vec![
                ("f2_1".into(), true.into()),
                ("f2_2".into(), (1.2_f32).into()),
            ]),
            Value::Record(vec![
                ("f2_1".into(), true.into()),
                ("f2_2".into(), (2.2_f32).into()),
            ]),
        ]),
    );
    record.put(
        "f3",
        Value::Union(
            1,
            Box::new(Value::Record(vec![("f3_1".into(), "xyz".into())])),
        ),
    );
    record.put(
        "f4",
        Value::Array(vec![
            Value::Union(
                1,
                Box::new(Value::Record(vec![("f4_1".into(), 200_i64.into())])),
            ),
            Value::Union(0, Box::new(Value::Null)),
        ]),
    );
    writer.append(record)?;

    let mut record = Record::new(writer.schema()).unwrap();
    record.put(
        "f1",
        Value::Record(vec![
            ("f1_1".into(), "bbb".into()),
            ("f1_2".into(), 20.into()),
            (
                "f1_3".into(),
                Value::Record(vec![("f1_3_1".into(), (3.140).into())]),
            ),
        ]),
    );
    record.put(
        "f2",
        Value::Array(vec![Value::Record(vec![
            ("f2_1".into(), false.into()),
            ("f2_2".into(), (10.2_f32).into()),
        ])]),
    );
    record.put("f3", Value::Union(0, Box::new(Value::Null)));
    record.put(
        "f4",
        Value::Array(vec![
            Value::Union(0, Box::new(Value::Null)),
            Value::Union(
                1,
                Box::new(Value::Record(vec![("f4_1".into(), 300_i64.into())])),
            ),
        ]),
    );
    writer.append(record)?;
    writer.flush()?;

    Ok(())
}
