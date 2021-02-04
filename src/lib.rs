#![allow(dead_code)]

mod encoding;
mod schema;

use std::fs::File;
use std::io::{self, BufReader, Read};
use std::path::Path;

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

#[derive(PartialEq, Debug)]
struct AvroDatafile {}

impl AvroDatafile {
    fn open<P: AsRef<Path>>(path: P) -> Result<Self, Error> {
        let file = File::open(path)?;
        let mut reader = BufReader::new(file);

        let mut header = [0; 4];
        reader.read_exact(&mut header)?;

        if header != [b'O', b'b', b'j', 1] {
            return Err(Error::InvalidFormat);
        }

        Ok(Self {})
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[ignore]
    fn read_booleans() {
        let _datafile = AvroDatafile::open("test_cases/booleans.avro").unwrap();
        // let objects: Vec<AvroValue> = datafile.objects().collect();
        // assert_eq!(objects, vec![AvroValue::Bool(true), AvroValue::Bool(false)]);
    }

    #[test]
    fn handle_invalid_avro_files() {
        let examples = [
            ("test_cases/nonexistent_file", Err(Error::IO(io::ErrorKind::NotFound))),
            ("test_cases/non_avro_file", Err(Error::InvalidFormat)),
        ];

        for (filename, expected) in examples.iter() {
            let actual = AvroDatafile::open(filename);
            assert_eq!(actual, *expected);
        }
    }
}
