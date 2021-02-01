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
    Union(Vec<SchemaType>),
    Reference(NamedTypeId),
}

#[derive(Debug, PartialEq)]
struct Field {
    name: String,
    schema_type: SchemaType,
}

#[derive(Debug, PartialEq)]
enum NamedType {
    Fixed(usize),
    Enum(Vec<String>),
    Record(Vec<Field>),
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
                    "record" => Self::parse_record(attributes, named_types),
                    _ => Self::match_primitive_typename(typename),
                },
                _ => Err(Error::InvalidSchema),
            },
            Value::Array(types) => Self::parse_union(types, named_types),
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

    fn parse_record(
        attributes: &Map<String, Value>,
        named_types: &mut NameRegistry,
    ) -> Result<Self, Error> {
        let name = match attributes.get("name") {
            Some(Value::String(name)) => Ok(name),
            _ => Err(Error::InvalidType),
        }?;

        let fields = match attributes.get("fields") {
            Some(Value::Array(fields)) => {
                let fields = fields
                    .iter()
                    .map(|field| match field {
                        Value::Object(field_attrs) => Self::parse_field(field_attrs, named_types),
                        _ => Err(Error::InvalidType),
                    })
                    .collect::<Result<Vec<Field>, Error>>()?;

                // If this redundant?
                Ok(fields)
            }
            _ => Err(Error::InvalidType),
        }?;

        let id = named_types.add_type(name, NamedType::Record(fields));
        Ok(SchemaType::Reference(id))
    }

    fn parse_field(
        attributes: &Map<String, Value>,
        named_types: &mut NameRegistry,
    ) -> Result<Field, Error> {
        let name = match attributes.get("name") {
            Some(Value::String(name)) => Ok(name.clone()),
            _ => Err(Error::InvalidType),
        }?;

        let schema_type = match attributes.get("type") {
            Some(field_type) => Self::parse(field_type, named_types),
            None => Err(Error::InvalidSchema),
        }?;

        Ok(Field { name, schema_type })
    }

    fn parse_union(types: &[Value], named_types: &mut NameRegistry) -> Result<Self, Error> {
        let union_types = types
            .iter()
            .map(|schema| Self::parse(schema, named_types))
            .collect::<Result<Vec<SchemaType>, Error>>()?;

        Ok(SchemaType::Union(union_types))
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

    #[test]
    fn parse_record() {
        let json_str = r#"{
          "type": "record",
          "name": "user",
          "fields": [
            {"name": "id", "type": "long"},
            {"name": "email", "type": "string"}
          ]
        }"#;

        let expected_type_def = NamedType::Record(vec![
            Field {
                name: "id".to_string(),
                schema_type: SchemaType::Long,
            },
            Field {
                name: "email".to_string(),
                schema_type: SchemaType::String,
            },
        ]);

        let json: Value = serde_json::from_str(json_str).unwrap();
        let mut named_types = NameRegistry::new();

        if let Ok(SchemaType::Reference(id)) = SchemaType::parse(&json, &mut named_types) {
            assert_eq!(named_types.get(id), Some(&expected_type_def));
        } else {
            panic!("parse should have returned a reference");
        }
    }

    #[test]
    fn parse_nested_record() {
        let json_str = r#"{
          "type": "record",
          "name": "user",
          "fields": [
            {
              "name": "name",
              "type": {
                "type": "record",
                "name": "fullname",
                "fields": [
                  {"name": "firstname", "type": "string"},
                  {"name": "lastname", "type": "string"}
                ]
              }
            }
          ]
        }"#;

        let json: Value = serde_json::from_str(json_str).unwrap();
        let mut named_types = NameRegistry::new();

        let parsed_schema = SchemaType::parse(&json, &mut named_types);

        let user_type_def = match parsed_schema {
            Ok(SchemaType::Reference(user_type_id)) => named_types.get(user_type_id).unwrap(),
            _ => panic!("parse should have returned a reference"),
        };

        let name_field_schema_type = match user_type_def {
            NamedType::Record(fields) => {
                assert_eq!(fields.len(), 1);
                assert_eq!(&fields[0].name, "name");
                &fields[0].schema_type
            }
            _ => panic!("user type should be a record"),
        };

        let actual_fullname_type_def = match name_field_schema_type {
            SchemaType::Reference(fullname_type_id) => named_types.get(*fullname_type_id).unwrap(),
            _ => panic!("name field should have been a reference"),
        };

        let expected_fullname_type_def = NamedType::Record(vec![
            Field {
                name: "firstname".to_string(),
                schema_type: SchemaType::String,
            },
            Field {
                name: "lastname".to_string(),
                schema_type: SchemaType::String,
            },
        ]);

        assert_eq!(*actual_fullname_type_def, expected_fullname_type_def);
    }

    #[test]
    fn parse_union() {
        let json_str = r#"["null","string","long"]"#;
        let json: Value = serde_json::from_str(json_str).unwrap();

        let mut named_types = NameRegistry::new();
        let actual = SchemaType::parse(&json, &mut named_types);

        let expected = Ok(SchemaType::Union(vec![
            SchemaType::Null,
            SchemaType::String,
            SchemaType::Long,
        ]));
        assert_eq!(actual, expected);
    }
}
