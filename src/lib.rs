#![allow(dead_code)]

mod encoding;
mod schema;

use schema::{NamedType, Schema, SchemaType};
use std::fs::File;
use std::io::{self, BufReader, Read};
use std::path::Path;

#[derive(PartialEq, Debug)]
enum AvroValue<'a> {
    Null,
    Boolean(bool),
    Int(i32),
    Long(i64),
    String(String),
    Enum(&'a str),
}

#[derive(PartialEq, Debug)]
enum Error {
    IO(io::ErrorKind),
    InvalidFormat,
    BadEncoding,
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Error {
        Error::IO(e.kind())
    }
}

struct SchemaRegistry {
    schemas: Vec<Schema>,
}

impl SchemaRegistry {
    fn new() -> Self {
        Self { schemas: Vec::new() }
    }

    // TODO: This should fingerprint the schemas and avoid saving
    // duplicates. Using a naive implementation for now since we need some
    // way to store schemas outside of the datafile struct.
    fn register(&mut self, schema: Schema) -> &Schema {
        self.schemas.push(schema);
        self.schemas.last().unwrap()
    }
}

type SyncMarker = [u8; 16];

#[derive(Debug)]
struct AvroDatafile<'a> {
    schema: &'a Schema,
    sync_marker: SyncMarker,
    reader: BufReader<File>,
    position: ReaderPosition,
}

impl<'a> AvroDatafile<'a> {
    fn open<P: AsRef<Path>>(path: P, schema_registry: &'a mut SchemaRegistry) -> Result<Self, Error> {
        let file = File::open(path)?;
        let mut reader = BufReader::new(file);

        let mut header = [0; 4];
        reader.read_exact(&mut header)?;

        if header != [b'O', b'b', b'j', 1] {
            return Err(Error::InvalidFormat);
        }

        let metadata = encoding::read_metadata(&mut reader)?;
        let schema_str = metadata.get("avro.schema").ok_or(Error::InvalidFormat)?;
        let schema = Schema::parse(&schema_str).map_err(|_| Error::InvalidFormat)?;
        let schema = schema_registry.register(schema);

        let mut sync_marker: SyncMarker = [0; 16];
        reader.read_exact(&mut sync_marker)?;

        Ok(Self {
            schema,
            sync_marker,
            reader,
            position: ReaderPosition::StartOfDataBlock,
        })
    }

    fn read_value<R: Read>(
        reader: &mut R,
        schema_type: &'a SchemaType,
        schema: &'a Schema,
    ) -> Result<AvroValue<'a>, Error> {
        match schema_type {
            SchemaType::Null => Ok(AvroValue::Null),
            SchemaType::Boolean => Ok(AvroValue::Boolean(encoding::read_bool(reader)?)),
            SchemaType::Int => Ok(AvroValue::Int(encoding::read_long(reader)? as i32)),
            SchemaType::Long => Ok(AvroValue::Long(encoding::read_long(reader)?)),
            SchemaType::String => Ok(AvroValue::String(encoding::read_string(reader)?)),
            SchemaType::Reference(id) => {
                let schema_type = schema.resolve_named_type(*id);

                match schema_type {
                    NamedType::Enum(values) => Ok(AvroValue::Enum(Self::read_enum_value(reader, &values)?)),
                    _ => Err(Error::BadEncoding),
                }
            }
            _ => Err(Error::BadEncoding),
        }
    }

    fn read_enum_value<R: Read>(reader: &mut R, values: &'a [String]) -> Result<&'a str, Error> {
        let index = encoding::read_long(reader)?;

        if index >= 0 && (index as usize) < values.len() {
            Ok(values[index as usize].as_ref())
        } else {
            Err(Error::BadEncoding)
        }
    }
}

#[derive(Debug)]
enum ReaderPosition {
    StartOfDataBlock,
    InDataBlock { remaining_object_count: u64 },
}

impl<'a> Iterator for AvroDatafile<'a> {
    type Item = Result<AvroValue<'a>, Error>;

    fn next(&mut self) -> Option<Result<AvroValue<'a>, Error>> {
        match self.position {
            ReaderPosition::StartOfDataBlock => {
                let objects_in_block = match encoding::read_long(&mut self.reader) {
                    Ok(object_count) => object_count as u64,
                    Err(Error::IO(io::ErrorKind::UnexpectedEof)) => return None,
                    Err(e) => return Some(Err(e)),
                };

                let _byte_length = match encoding::read_long(&mut self.reader) {
                    Ok(byte_length) => byte_length,
                    Err(e) => return Some(Err(e)),
                };

                self.position = ReaderPosition::InDataBlock {
                    remaining_object_count: objects_in_block,
                };

                self.next()
            }
            ReaderPosition::InDataBlock { remaining_object_count } => {
                if remaining_object_count > 0 {
                    let value = Self::read_value(&mut self.reader, self.schema.root(), self.schema);
                    self.position = ReaderPosition::InDataBlock {
                        remaining_object_count: remaining_object_count - 1,
                    };
                    Some(value)
                } else {
                    let mut sync_marker: SyncMarker = [0; 16];
                    if let Err(e) = self.reader.read_exact(&mut sync_marker) {
                        return Some(Err(Error::IO(e.kind())));
                    }

                    if sync_marker != self.sync_marker {
                        return Some(Err(Error::BadEncoding));
                    }

                    self.position = ReaderPosition::StartOfDataBlock;
                    self.next()
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_datafiles() {
        let examples = [
            ("test_cases/null.avro", vec![AvroValue::Null, AvroValue::Null]),
            (
                "test_cases/boolean.avro",
                vec![AvroValue::Boolean(true), AvroValue::Boolean(false)],
            ),
            (
                "test_cases/int.avro",
                vec![
                    AvroValue::Int(42),
                    AvroValue::Int(-100),
                    AvroValue::Int(0),
                    AvroValue::Int(2147483647),
                    AvroValue::Int(-2147483648),
                ],
            ),
            (
                "test_cases/long.avro",
                vec![
                    AvroValue::Long(42),
                    AvroValue::Long(-100),
                    AvroValue::Long(0),
                    AvroValue::Long(-9223372036854775808),
                    AvroValue::Long(9223372036854775807),
                ],
            ),
            (
                "test_cases/string.avro",
                vec![
                    AvroValue::String("foo".to_string()),
                    AvroValue::String("bar".to_string()),
                    AvroValue::String("".to_string()),
                    AvroValue::String("\u{263A}".to_string()),
                ],
            ),
            (
                "test_cases/enum.avro",
                vec![
                    AvroValue::Enum("clubs"),
                    AvroValue::Enum("hearts"),
                    AvroValue::Enum("spades"),
                ],
            ),
        ];

        for (filename, expected_values) in examples.iter() {
            let mut schema_registry = SchemaRegistry::new();
            let datafile = AvroDatafile::open(filename, &mut schema_registry).unwrap();
            let actual_values: Vec<AvroValue> = datafile.collect::<Result<_, Error>>().unwrap();
            assert_eq!(actual_values, *expected_values);
        }
    }

    #[test]
    fn handle_invalid_avro_files() {
        let examples = [
            ("test_cases/nonexistent_file", Error::IO(io::ErrorKind::NotFound)),
            ("test_cases/non_avro_file", Error::InvalidFormat),
        ];

        for (filename, expected_err) in examples.iter() {
            let mut schema_registry = SchemaRegistry::new();
            let result = AvroDatafile::open(filename, &mut schema_registry);
            assert!(result.is_err());
            assert_eq!(result.unwrap_err(), *expected_err);
        }
    }
}
