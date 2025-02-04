use serde::{Deserialize, Serialize};

#[derive(Serialize, Debug, Clone, PartialEq)]
enum MicroOperation {
    // #[serde(rename = "r")]
    Read { key: usize, value: Option<Vec<i64>> },
    Append { key: usize, value: i64 },
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
                            .next_element::<Option<Vec<i64>>>()?
                            .expect("read txn op should contain a op,k,v . Here, the v is missing");
                        if value.is_some() {
                            panic!("expected null for read txn value. got {:?}", value);
                        }
                        return Ok(MicroOperation::Read { key, value: None });
                    }
                    "append" => {
                        let value = seq
                            .next_element::<i64>()?
                            .expect("append txn should have an i64 value to append");

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
}
