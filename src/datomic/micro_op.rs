use core::panic;

use serde::{ser::SerializeSeq, Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum MicroOperation {
    Read {
        key: usize,
        value: Option<Vec<usize>>,
    },
    Append {
        key: usize,
        value: usize,
    },
}

impl Serialize for MicroOperation {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut seq = serializer.serialize_seq(Some(3))?;

        match self {
            MicroOperation::Read { key, value } => {
                seq.serialize_element("r")?;
                seq.serialize_element(key)?;
                seq.serialize_element(value)?;
            }
            MicroOperation::Append { key, value } => {
                seq.serialize_element("append")?;
                seq.serialize_element(key)?;
                seq.serialize_element(value)?;
            }
        }

        seq.end()
    }
}

impl<'de> Deserialize<'de> for MicroOperation {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct MicroOperationVisitor;

        impl<'de> serde::de::Visitor<'de> for MicroOperationVisitor {
            type Value = MicroOperation;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a JSON array [op,k,v]. See maelstrom txn-list-append workload")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::SeqAccess<'de>,
            {
                let operation = seq
                    .next_element::<&str>()?
                    .expect("txn starts with an operation. e.g. 'r'");
                let key = seq
                    .next_element::<usize>()?
                    .expect("txn op is followed by a key. e.g. 'r', 1");

                match operation {
                    "r" => {
                        let value = seq
                            .next_element::<Option<Vec<usize>>>()?
                            .expect("read txn op should contain a op,k,v . Here, the v is missing");
                        if value.is_some() {
                            panic!("expected null for read txn value. got {:?}", value);
                        }
                        return Ok(MicroOperation::Read { key, value: None });
                    }
                    "append" => {
                        let value = seq
                            .next_element::<usize>()?
                            .expect("append txn should have an usize value to append");

                        return Ok(MicroOperation::Append { key, value });
                    }
                    unsupported_micro_op => {
                        panic!("micro_op unsupported: {unsupported_micro_op}")
                    }
                }
            }
        }
        const FIELDS: &[&str] = &["r", "append"];

        deserializer.deserialize_struct("MicroOperation", FIELDS, MicroOperationVisitor)
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn test_deserialization() -> Result<(), serde_json::Error> {
        let micro_op: MicroOperation = serde_json::from_str(r#"["r", 5, null]"#)?;

        assert_eq!(
            micro_op,
            MicroOperation::Read {
                key: 5,
                value: None
            }
        );

        let micro_op: MicroOperation = serde_json::from_str(r#"["append", 1, 3]"#)?;

        assert_eq!(micro_op, MicroOperation::Append { key: 1, value: 3 });

        Ok(())
    }

    #[test]
    fn test_serialization() -> Result<(), serde_json::Error> {
        let read_op = MicroOperation::Read {
            key: 5,
            value: Some(vec![10, 20]),
        };

        let serialized = serde_json::to_value(&read_op)?;
        assert_eq!(serialized, json!(["r", 5, [10, 20]]));

        Ok(())
    }
}
