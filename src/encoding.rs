use crate::Error;
use std::collections::HashMap;
use std::io::Read;

pub(crate) fn read_bool<R: Read>(reader: &mut R) -> Result<bool, Error> {
    Ok(read_byte(reader)? == 1)
}

pub(crate) fn read_long<R: Read>(reader: &mut R) -> Result<i64, Error> {
    Ok(read_varint_long(reader).map(decode_zigzag_long)?)
}

fn decode_zigzag_long(encoded_value: u64) -> i64 {
    ((encoded_value >> 1) as i64) ^ -((encoded_value & 1) as i64)
}

fn read_varint_long<R: Read>(reader: &mut R) -> Result<u64, Error> {
    let mut byte = read_byte(reader)?;
    let mut accum: u64 = (byte & 0b0111_1111) as u64;
    let mut shift = 0;

    while byte & 0b1000_0000 != 0 {
        byte = read_byte(reader)?;

        shift += 7;
        if shift >= 64 {
            return Err(Error::BadEncoding);
        }

        accum += ((byte & 0b0111_1111) as u64) << shift;
    }

    Ok(accum)
}

fn read_byte<R: Read>(reader: &mut R) -> Result<u8, Error> {
    let mut buffer: [u8; 1] = [0];
    reader.read_exact(&mut buffer)?;
    Ok(buffer[0])
}

pub(crate) fn read_string<R: Read>(reader: &mut R) -> Result<String, Error> {
    let byte_length = read_long(reader)? as usize;
    let mut buffer = vec![0; byte_length];
    reader.read_exact(&mut buffer)?;
    String::from_utf8(buffer).map_err(|_| Error::BadEncoding)
}

pub(crate) fn read_metadata<R: Read>(reader: &mut R) -> Result<HashMap<String, String>, Error> {
    let mut metadata: HashMap<String, String> = HashMap::new();
    let mut num_values = read_block_count(reader)?;

    while num_values > 0 {
        for _ in 0..num_values {
            let key = read_string(reader)?;
            let value = read_string(reader)?;

            metadata.insert(key, value);
        }

        num_values = read_block_count(reader)?;
    }

    Ok(metadata)
}

fn read_block_count<R: Read>(reader: &mut R) -> Result<i64, Error> {
    let num_values = read_long(reader)?;
    if num_values.is_negative() {
        let _block_size_in_bytes = read_long(reader)?;
        Ok(num_values.abs())
    } else {
        Ok(num_values)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::ErrorKind;

    #[test]
    fn read_unsigned_varint() {
        let examples = [
            (vec![0b0000_0000], Ok(0)),
            (vec![0b0000_0001], Ok(1)),
            (vec![0b0000_0010], Ok(2)),
            (vec![0b0111_1111], Ok(127)),
            (vec![0b1000_0000, 0b0000_0001], Ok(128)),
            (vec![0b1000_0001, 0b0000_0001], Ok(129)),
            (vec![0b1000_0010, 0b0000_0001], Ok(130)),
            (vec![0b1111_1111, 0b0111_1111], Ok(16_383)),
            (vec![0b1000_0000, 0b1000_0000, 0b0000_0001], Ok(16_384)),
            (vec![0b1000_0001, 0b1000_0000, 0b0000_0001], Ok(16_385)),
            (
                vec![0x81, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01],
                Ok(9_223_372_036_854_775_809u64),
            ),
            // A varint long that exceeds i64 range should return an error
            (
                vec![0x81, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01],
                Err(Error::BadEncoding),
            ),
        ];

        for (input, expected) in examples.iter() {
            let actual = read_varint_long(&mut input.as_slice());
            assert_eq!(actual, *expected);
        }
    }

    #[test]
    fn decode_zigzag_integers() {
        let examples: Vec<(i64, u64)> = vec![
            (0, 0),
            (-1, 1),
            (1, 2),
            (2147483647, 4294967294),
            (-2147483648, 4294967295),
        ];

        for (expected_value, encoded_value) in examples.iter() {
            let decoded_value = decode_zigzag_long(*encoded_value);
            assert_eq!(decoded_value, *expected_value);
        }
    }

    #[test]
    fn read_longs() {
        // Taken from the example table in the Avro 1.10.1 specification
        let input = vec![0x00, 0x01, 0x02, 0x03, 0x7f, 0x80, 0x01];
        let mut reader = input.as_slice();

        assert_eq!(read_long(&mut reader), Ok(0));
        assert_eq!(read_long(&mut reader), Ok(-1));
        assert_eq!(read_long(&mut reader), Ok(1));
        assert_eq!(read_long(&mut reader), Ok(-2));
        assert_eq!(read_long(&mut reader), Ok(-64));
        assert_eq!(read_long(&mut reader), Ok(64));
        assert_eq!(read_long(&mut reader), Err(Error::IO(ErrorKind::UnexpectedEof)));
    }

    #[test]
    fn read_bools() {
        let input = vec![0x00, 0x01, 0x00];
        let mut reader = input.as_slice();

        assert_eq!(read_bool(&mut reader), Ok(false));
        assert_eq!(read_bool(&mut reader), Ok(true));
        assert_eq!(read_bool(&mut reader), Ok(false));
        assert_eq!(read_bool(&mut reader), Err(Error::IO(ErrorKind::UnexpectedEof)));
    }

    #[test]
    fn read_strings() {
        let input = vec![0x06, 0x66, 0x6f, 0x6f, 0x0c, 0xe2, 0x98, 0x83, 0xe2, 0x98, 0x83];
        let mut reader = input.as_slice();

        assert_eq!(read_string(&mut reader), Ok("foo".to_string()));
        assert_eq!(read_string(&mut reader), Ok("☃☃".to_string()));
        assert_eq!(read_string(&mut reader), Err(Error::IO(ErrorKind::UnexpectedEof)));
    }

    #[test]
    fn read_metadata_map() {
        let input = vec![
            0x04, // 2 key value pairs in this block
            0x06, 0x66, 0x6f, 0x6f, // "foo"
            0x06, 0x62, 0x61, 0x72, // "bar"
            0x06, 0x62, 0x61, 0x7a, // "baz"
            0x06, 0x62, 0x61, 0x74, // "bat"
            0x01, // 1 key value pair in this block, encoded as -1 to also specify length
            0x18, // block is 12 bytes long
            0x0a, 0x68, 0x65, 0x6c, 0x6c, 0x6f, // "hello"
            0x0a, 0x77, 0x6f, 0x72, 0x6c, 0x64, // "world"
            0x00, // end with empty block
        ];

        let mut reader = input.as_slice();

        let metadata = read_metadata(&mut reader).unwrap();
        assert_eq!(metadata.len(), 3);
        assert_eq!(metadata.get("foo"), Some(&"bar".to_string()));
        assert_eq!(metadata.get("baz"), Some(&"bat".to_string()));
        assert_eq!(metadata.get("hello"), Some(&"world".to_string()));
    }
}
