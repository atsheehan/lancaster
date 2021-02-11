#![allow(dead_code)]

mod encoding;
mod schema;

use schema::{Schema, SchemaType};
use std::fs::File;
use std::io::{self, BufReader, Read};
use std::path::Path;

#[derive(PartialEq, Debug)]
enum AvroValue {
    Boolean(bool),
    String(String),
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

type SyncMarker = [u8; 16];

#[derive(Debug)]
struct AvroDatafile {
    schema: Schema,
    sync_marker: SyncMarker,
    reader: BufReader<File>,
    position: ReaderPosition,
}

impl AvroDatafile {
    fn open<P: AsRef<Path>>(path: P) -> Result<Self, Error> {
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

        let mut sync_marker: SyncMarker = [0; 16];
        reader.read_exact(&mut sync_marker)?;

        Ok(Self {
            schema,
            sync_marker,
            reader,
            position: ReaderPosition::StartOfDataBlock,
        })
    }

    fn read_value<R: Read>(reader: &mut R, schema_type: &SchemaType) -> Result<AvroValue, Error> {
        match schema_type {
            SchemaType::Boolean => Ok(AvroValue::Boolean(encoding::read_bool(reader)?)),
            SchemaType::String => Ok(AvroValue::String(encoding::read_string(reader)?)),
            _ => Err(Error::BadEncoding),
        }
    }
}

#[derive(Debug)]
enum ReaderPosition {
    StartOfDataBlock,
    InDataBlock { remaining_object_count: u64 },
}

impl Iterator for AvroDatafile {
    type Item = Result<AvroValue, Error>;

    fn next(&mut self) -> Option<Result<AvroValue, Error>> {
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
                    let value = Self::read_value(&mut self.reader, self.schema.root());
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
    fn read_booleans() {
        let datafile = AvroDatafile::open("test_cases/boolean.avro").unwrap();
        let expected = vec![AvroValue::Boolean(true), AvroValue::Boolean(false)];

        let actual: Vec<AvroValue> = datafile.collect::<Result<_, Error>>().unwrap();
        assert_eq!(actual, expected);
    }

    #[test]
    fn read_strings() {
        let datafile = AvroDatafile::open("test_cases/string.avro").unwrap();
        let expected = vec![
            AvroValue::String("foo".to_string()),
            AvroValue::String("bar".to_string()),
            AvroValue::String("".to_string()),
            AvroValue::String("\u{263A}".to_string()),
        ];

        let actual: Vec<AvroValue> = datafile.collect::<Result<_, Error>>().unwrap();
        assert_eq!(actual, expected);
    }

    #[test]
    fn handle_invalid_avro_files() {
        let examples = [
            ("test_cases/nonexistent_file", Error::IO(io::ErrorKind::NotFound)),
            ("test_cases/non_avro_file", Error::InvalidFormat),
        ];

        for (filename, expected_err) in examples.iter() {
            let result = AvroDatafile::open(filename);
            assert!(result.is_err());
            assert_eq!(result.unwrap_err(), *expected_err);
        }
    }
}
