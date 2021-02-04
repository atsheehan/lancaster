use crate::Error;
use std::io::Read;

fn read_long<R: Read>(reader: &mut R) -> Result<i64, Error> {
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
}
