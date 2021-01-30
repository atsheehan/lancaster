use serde_json::{Map, Value};

#[derive(Debug, PartialEq)]
pub enum SchemaType {
    Null,
    Boolean,
    Int,
    Long,
    Float,
    Double,
    Bytes,
    String,
    Array(Box<SchemaType>),
    Map(Box<SchemaType>),
}

impl SchemaType {
    pub fn parse(json: &Value) -> Result<Self, Error> {
        match json {
            Value::String(typename) => Self::match_primitive_typename(typename),
            Value::Object(attributes) => match attributes.get("type") {
                Some(Value::String(typename)) => match typename.as_ref() {
                    "array" => Self::parse_array(attributes),
                    "map" => Self::parse_map(attributes),
                    _ => Self::match_primitive_typename(typename),
                },
                _ => Err(Error::InvalidSchema),
            },
            _ => Err(Error::InvalidSchema),
        }
    }

    fn parse_array(attributes: &Map<String, Value>) -> Result<Self, Error> {
        match attributes.get("items") {
            Some(item_type) => Ok(SchemaType::Array(Box::new(Self::parse(item_type)?))),
            None => Err(Error::InvalidSchema),
        }
    }

    fn parse_map(attributes: &Map<String, Value>) -> Result<Self, Error> {
        match attributes.get("values") {
            Some(item_type) => Ok(SchemaType::Map(Box::new(Self::parse(item_type)?))),
            None => Err(Error::InvalidSchema),
        }
    }

    fn match_primitive_typename(typename: &str) -> Result<Self, Error> {
        match typename {
            "null" => Ok(SchemaType::Null),
            "boolean" => Ok(SchemaType::Boolean),
            "int" => Ok(SchemaType::Int),
            "long" => Ok(SchemaType::Long),
            "float" => Ok(SchemaType::Float),
            "double" => Ok(SchemaType::Double),
            "bytes" => Ok(SchemaType::Bytes),
            "string" => Ok(SchemaType::String),
            _ => Err(Error::UnrecognizedType),
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum Error {
    UnrecognizedType,
    InvalidSchema,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_primitive_types() {
        let examples = [
            (r#""null""#, Ok(SchemaType::Null)),
            (r#""boolean""#, Ok(SchemaType::Boolean)),
            (r#""int""#, Ok(SchemaType::Int)),
            (r#""long""#, Ok(SchemaType::Long)),
            (r#""float""#, Ok(SchemaType::Float)),
            (r#""double""#, Ok(SchemaType::Double)),
            (r#""bytes""#, Ok(SchemaType::Bytes)),
            (r#""string""#, Ok(SchemaType::String)),
            (r#""option""#, Err(Error::UnrecognizedType)),
            (r#"{"type": "string"}"#, Ok(SchemaType::String)),
            (r#"null"#, Err(Error::InvalidSchema)),
            (r#"true"#, Err(Error::InvalidSchema)),
        ];

        for (json_str, expected) in examples.iter() {
            let json: Value = serde_json::from_str(json_str).unwrap();
            let actual = SchemaType::parse(&json);
            assert_eq!(actual, *expected);
        }
    }

    #[test]
    fn parse_arrays_and_maps() {
        let examples = [
            (
                r#"{"type": "array", "items": "string"}"#,
                Ok(SchemaType::Array(Box::new(SchemaType::String))),
            ),
            (
                r#"{"type": "array", "items": {"type": "array", "items": "string"}}"#,
                Ok(SchemaType::Array(Box::new(SchemaType::Array(Box::new(
                    SchemaType::String,
                ))))),
            ),
            (r#"{"type": "array"}"#, Err(Error::InvalidSchema)),
            (
                r#"{"type": "map", "values": "long"}"#,
                Ok(SchemaType::Map(Box::new(SchemaType::Long))),
            ),
            (
                r#"{"type": "map", "values": {"type": "map", "values": "long"}}"#,
                Ok(SchemaType::Map(Box::new(SchemaType::Map(Box::new(
                    SchemaType::Long,
                ))))),
            ),
            (r#"{"type": "map"}"#, Err(Error::InvalidSchema)),
        ];

        for (json_str, expected) in examples.iter() {
            let json: Value = serde_json::from_str(json_str).unwrap();
            let actual = SchemaType::parse(&json);
            assert_eq!(actual, *expected);
        }
    }
}
