#![allow(dead_code)]

use serde_json::{Map, Value};
use std::collections::HashMap;

type NamedTypeId = usize;

#[derive(Debug, PartialEq)]
enum SchemaType {
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
    Reference(NamedTypeId),
}

#[derive(Debug, PartialEq)]
enum NamedType {
    Fixed(usize),
    Enum(Vec<String>),
}

struct NameRegistry {
    type_definitions: Vec<Option<NamedType>>,
    name_to_id_mappings: HashMap<String, NamedTypeId>,
}

impl NameRegistry {
    fn new() -> Self {
        Self {
            type_definitions: Vec::new(),
            name_to_id_mappings: HashMap::new(),
        }
    }

    fn get(&self, id: NamedTypeId) -> Option<&NamedType> {
        match self.type_definitions.get(id) {
            Some(Some(value)) => Some(value),
            _ => None,
        }
    }

    fn add_type(&mut self, name: &str, definition: NamedType) -> NamedTypeId {
        let id = self.type_definitions.len();
        self.type_definitions.push(Some(definition));
        self.name_to_id_mappings.insert(name.to_string(), id);
        id
    }
}

impl SchemaType {
    fn parse(json: &Value, named_types: &mut NameRegistry) -> Result<Self, Error> {
        match json {
            Value::String(typename) => Self::match_primitive_typename(typename),
            Value::Object(attributes) => match attributes.get("type") {
                Some(Value::String(typename)) => match typename.as_ref() {
                    "array" => Self::parse_array(attributes, named_types),
                    "map" => Self::parse_map(attributes, named_types),
                    "fixed" => Self::parse_fixed(attributes, named_types),
                    "enum" => Self::parse_enum(attributes, named_types),
                    _ => Self::match_primitive_typename(typename),
                },
                _ => Err(Error::InvalidSchema),
            },
            _ => Err(Error::InvalidSchema),
        }
    }

    fn parse_array(
        attributes: &Map<String, Value>,
        named_types: &mut NameRegistry,
    ) -> Result<Self, Error> {
        match attributes.get("items") {
            Some(item_type) => Ok(SchemaType::Array(Box::new(Self::parse(
                item_type,
                named_types,
            )?))),
            None => Err(Error::InvalidSchema),
        }
    }

    fn parse_map(
        attributes: &Map<String, Value>,
        named_types: &mut NameRegistry,
    ) -> Result<Self, Error> {
        match attributes.get("values") {
            Some(item_type) => Ok(SchemaType::Map(Box::new(Self::parse(
                item_type,
                named_types,
            )?))),
            None => Err(Error::InvalidSchema),
        }
    }

    fn parse_fixed(
        attributes: &Map<String, Value>,
        named_types: &mut NameRegistry,
    ) -> Result<Self, Error> {
        let name = match attributes.get("name") {
            Some(Value::String(name)) => Ok(name),
            _ => Err(Error::InvalidType),
        }?;

        let size = match attributes.get("size") {
            Some(Value::Number(size)) => {
                let size = size.as_u64().ok_or(Error::InvalidType)?;
                Ok(size as usize)
            }
            _ => Err(Error::InvalidType),
        }?;

        let id = named_types.add_type(name, NamedType::Fixed(size));
        Ok(SchemaType::Reference(id))
    }

    fn parse_enum(
        attributes: &Map<String, Value>,
        named_types: &mut NameRegistry,
    ) -> Result<Self, Error> {
        let name = match attributes.get("name") {
            Some(Value::String(name)) => Ok(name),
            _ => Err(Error::InvalidType),
        }?;

        let symbols = match attributes.get("symbols") {
            Some(Value::Array(symbols)) => {
                let symbols = symbols
                    .iter()
                    .map(|v| match v {
                        Value::String(s) => Ok(s.clone()),
                        _ => Err(Error::InvalidType),
                    })
                    .collect::<Result<Vec<String>, Error>>()?;

                Ok(symbols)
            }
            _ => Err(Error::InvalidType),
        }?;

        let id = named_types.add_type(name, NamedType::Enum(symbols));
        Ok(SchemaType::Reference(id))
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
    InvalidType,
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
            let mut named_types = NameRegistry::new();

            let actual = SchemaType::parse(&json, &mut named_types);
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
            let mut named_types = NameRegistry::new();

            let actual = SchemaType::parse(&json, &mut named_types);
            assert_eq!(actual, *expected);
        }
    }

    #[test]
    fn parse_enums_and_fixed_types() {
        let valid_examples = [
            (
                r#"{"type": "fixed", "name": "blob", "size": 42}"#,
                Some(NamedType::Fixed(42)),
            ),
            (
                r#"{
                     "type": "enum", "name": "suit",
                      "symbols": ["HEARTS", "CLUBS", "SPADES", "DIAMONDS"]
                   }"#,
                Some(NamedType::Enum(vec![
                    "HEARTS".to_string(),
                    "CLUBS".to_string(),
                    "SPADES".to_string(),
                    "DIAMONDS".to_string(),
                ])),
            ),
        ];

        for (json_str, expected_type_def) in valid_examples.iter() {
            let json: Value = serde_json::from_str(json_str).unwrap();
            let mut named_types = NameRegistry::new();

            if let Ok(SchemaType::Reference(id)) = SchemaType::parse(&json, &mut named_types) {
                assert_eq!(named_types.get(id), expected_type_def.as_ref());
            } else {
                panic!("parse should have returned a reference");
            }
        }

        let invalid_examples = [
            (
                r#"{"type": "fixed", "name": "blob"}"#,
                Err(Error::InvalidType),
            ),
            (r#"{"type": "fixed", "size": 42}"#, Err(Error::InvalidType)),
            (
                r#"{"type": "enum", "symbols": ["foo"]}"#,
                Err(Error::InvalidType),
            ),
            (
                r#"{"type": "enum", "name": "suit"}"#,
                Err(Error::InvalidType),
            ),
            (
                r#"{"type": "enum", "name": "suit", "symbols": "diamonds"}"#,
                Err(Error::InvalidType),
            ),
        ];

        for (json_str, expected_error) in invalid_examples.iter() {
            let json: Value = serde_json::from_str(json_str).unwrap();
            let mut named_types = NameRegistry::new();

            let actual = SchemaType::parse(&json, &mut named_types);
            assert_eq!(actual, *expected_error);
        }
    }
}
