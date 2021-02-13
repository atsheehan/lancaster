#![allow(dead_code)]

mod encoding;
mod schema;

use schema::{Field, NamedType, Schema, SchemaType};
use std::collections::HashMap;
use std::fs::File;
use std::io::{self, BufReader, Read};
use std::path::Path;

#[derive(PartialEq, Debug)]
enum AvroValue<'a> {
    Null,
    Boolean(bool),
    Int(i32),
    Long(i64),
    Float(f32),
    Double(f64),
    String(String),
    Bytes(Vec<u8>),
    Array(Vec<AvroValue<'a>>),
    Map(HashMap<String, AvroValue<'a>>),
    Enum(&'a str),
    Fixed(Vec<u8>),
    Record(HashMap<&'a str, AvroValue<'a>>),
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
            SchemaType::Float => Ok(AvroValue::Float(encoding::read_float(reader)?)),
            SchemaType::Double => Ok(AvroValue::Double(encoding::read_double(reader)?)),
            SchemaType::Bytes => Ok(AvroValue::Bytes(encoding::read_bytes(reader)?)),
            SchemaType::String => Ok(AvroValue::String(encoding::read_string(reader)?)),
            SchemaType::Union(types) => Ok(Self::read_union(reader, types, schema)?),
            SchemaType::Array(item_type) => Ok(AvroValue::Array(Self::read_array(reader, item_type, schema)?)),
            SchemaType::Map(value_type) => Ok(AvroValue::Map(Self::read_map(reader, value_type, schema)?)),
            SchemaType::Reference(id) => {
                let schema_type = schema.resolve_named_type(*id);

                match schema_type {
                    NamedType::Enum(values) => Ok(AvroValue::Enum(Self::read_enum_value(reader, &values)?)),
                    NamedType::Fixed(size) => Ok(AvroValue::Fixed(encoding::read_fixed(reader, *size)?)),
                    NamedType::Record(fields) => Ok(AvroValue::Record(Self::read_fields(reader, fields, schema)?)),
                }
            }
        }
    }

    fn read_union<R: Read>(
        reader: &mut R,
        possible_types: &'a [SchemaType],
        schema: &'a Schema,
    ) -> Result<AvroValue<'a>, Error> {
        let index = encoding::read_long(reader)?;

        if index >= 0 && (index as usize) < possible_types.len() {
            Self::read_value(reader, &possible_types[index as usize], schema)
        } else {
            Err(Error::InvalidFormat)
        }
    }

    fn read_array<R: Read>(
        reader: &mut R,
        item_type: &'a SchemaType,
        schema: &'a Schema,
    ) -> Result<Vec<AvroValue<'a>>, Error> {
        let mut num_values = encoding::read_long(reader)?;
        let mut values = Vec::with_capacity(num_values as usize);

        while num_values != 0 {
            for _ in 0..num_values {
                values.push(Self::read_value(reader, item_type, schema)?);
            }

            num_values = encoding::read_long(reader)?;
        }

        Ok(values)
    }

    fn read_map<R: Read>(
        reader: &mut R,
        value_type: &'a SchemaType,
        schema: &'a Schema,
    ) -> Result<HashMap<String, AvroValue<'a>>, Error> {
        // TODO: handle negative num values
        let mut num_values = encoding::read_long(reader)?;
        let mut entries: HashMap<String, AvroValue<'a>> = HashMap::with_capacity(num_values as usize);

        while num_values > 0 {
            for _ in 0..num_values {
                let key = encoding::read_string(reader)?;
                let value = Self::read_value(reader, value_type, schema)?;

                entries.insert(key, value);
            }

            num_values = encoding::read_long(reader)?;
        }

        Ok(entries)
    }

    fn read_enum_value<R: Read>(reader: &mut R, values: &'a [String]) -> Result<&'a str, Error> {
        let index = encoding::read_long(reader)?;

        if index >= 0 && (index as usize) < values.len() {
            Ok(values[index as usize].as_ref())
        } else {
            Err(Error::BadEncoding)
        }
    }

    fn read_fields<R: Read>(
        reader: &mut R,
        fields: &'a [Field],
        schema: &'a Schema,
    ) -> Result<HashMap<&'a str, AvroValue<'a>>, Error> {
        let mut field_values = HashMap::with_capacity(fields.len());

        for field in fields {
            let value = Self::read_value(reader, field.schema_type(), schema)?;
            field_values.insert(field.name(), value);
        }

        Ok(field_values)
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
                "test_cases/float.avro",
                vec![
                    AvroValue::Float(std::f32::consts::PI),
                    AvroValue::Float(0.0),
                    AvroValue::Float(3.402_823_5E38),
                    AvroValue::Float(-3.402_823_5E38),
                ],
            ),
            (
                "test_cases/double.avro",
                vec![
                    AvroValue::Double(0.0),
                    AvroValue::Double(std::f64::MAX),
                    AvroValue::Double(std::f64::MIN),
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
                "test_cases/bytes.avro",
                vec![AvroValue::Bytes(vec![1, 2, 3]), AvroValue::Bytes(vec![0xff, 0x01])],
            ),
            ("test_cases/union.avro", vec![AvroValue::Null, AvroValue::Boolean(true)]),
            (
                "test_cases/array.avro",
                vec![
                    AvroValue::Array(vec![AvroValue::Int(1), AvroValue::Int(2), AvroValue::Int(3)]),
                    AvroValue::Array(vec![AvroValue::Int(-10), AvroValue::Int(-20)]),
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
            (
                "test_cases/fixed.avro",
                vec![AvroValue::Fixed(vec![1, 2, 3, 4]), AvroValue::Fixed(vec![5, 6, 7, 8])],
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
    fn read_maps_from_file() {
        // There isn't an easy way to define hashmap literals in the
        // previous test, so pulling this out as a separate test.
        let mut first = HashMap::new();
        first.insert("foo".to_string(), AvroValue::Int(1));
        first.insert("bar".to_string(), AvroValue::Int(2));

        let mut second = HashMap::new();
        second.insert("hi".to_string(), AvroValue::Int(-1));

        let expected_values = vec![AvroValue::Map(first), AvroValue::Map(second)];

        let mut schema_registry = SchemaRegistry::new();
        let datafile = AvroDatafile::open("test_cases/map.avro", &mut schema_registry).unwrap();
        let actual_values: Vec<AvroValue> = datafile.collect::<Result<_, Error>>().unwrap();
        assert_eq!(actual_values, expected_values);
    }

    #[test]
    fn read_records_from_file() {
        let mut first = HashMap::new();
        first.insert("email", AvroValue::String("bloblaw@example.com".to_string()));
        first.insert("age", AvroValue::Int(42));

        let mut second = HashMap::new();
        second.insert("email", AvroValue::String("gmbluth@example.com".to_string()));
        second.insert("age", AvroValue::Int(16));

        let expected_values = vec![AvroValue::Record(first), AvroValue::Record(second)];

        let mut schema_registry = SchemaRegistry::new();
        let datafile = AvroDatafile::open("test_cases/record.avro", &mut schema_registry).unwrap();
        let actual_values: Vec<AvroValue> = datafile.collect::<Result<_, Error>>().unwrap();
        assert_eq!(actual_values, expected_values);
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
