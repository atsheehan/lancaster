@Grab('org.apache.avro:avro:1.10.1')
import groovy.json.JsonOutput
import org.apache.avro.Schema
import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.generic.GenericData
import org.apache.avro.file.CodecFactory
import org.apache.avro.file.DataFileWriter
import java.nio.ByteBuffer;

def dir = new File("test_cases")

if (!dir.exists() || !dir.isDirectory()) {
    println("FATAL: missing test_cases directory")
    System.exit(1)
}

def parseSchema(jsonSchema) {
    new Schema.Parser().parse(jsonSchema)
}

def writeAvroFile(dir, filename, schema, data) {
    writeAvroFileWithCodec(dir, filename, schema, data, CodecFactory.nullCodec())
}

def writeAvroFileWithCodec(dir, filename, schema, data, codec) {
    new File(dir, filename).withOutputStream { stream ->
        def datumWriter = new GenericDatumWriter(schema)
        def dataFileWriter = new DataFileWriter(datumWriter)
        def syncMarker = "abcdefghijklmnop".getBytes()
        dataFileWriter.setCodec(codec)
        dataFileWriter.create(schema, stream, syncMarker)

        data.each {
            dataFileWriter.append(it)
        }

        dataFileWriter.close()
    }
}

writeAvroFile(dir, "boolean.avro", parseSchema('"boolean"'), [true, false])
writeAvroFile(dir, "int.avro", parseSchema('"int"'), [42, -100, 0, 2147483647, -2147483648])
writeAvroFile(dir, "long.avro", parseSchema('"long"'), [42, -100, 0, -9223372036854775808, 9223372036854775807])
writeAvroFile(dir, "float.avro", parseSchema('"float"'), [3.1415927, 0.0, 3.40282347E+38, -3.40282347E+38])
writeAvroFile(dir, "double.avro", parseSchema('"double"'), [0.0, 1.7976931348623157E+308, -1.7976931348623157E+308])
writeAvroFile(dir, "string.avro", parseSchema('"string"'), ["foo", "bar", "", "\u263A"])
writeAvroFile(dir, "bytes.avro", parseSchema('"bytes"'), [ByteBuffer.wrap([1, 2, 3] as byte[]), ByteBuffer.wrap([0xff, 0x01] as byte[])])
writeAvroFile(dir, "null.avro", parseSchema('"null"'), [null, null])
writeAvroFile(dir, "array.avro", parseSchema('{"type":"array", "items":"int"}'), [[1, 2, 3], [-10, -20]])
writeAvroFile(dir, "map.avro", parseSchema('{"type":"map", "values":"int"}'), [["foo": 1, "bar": 2], ["hi": -1]])
writeAvroFile(dir, "union.avro", parseSchema('["null", "boolean"]'), [null, true])

def fixedSchema = parseSchema("""
{
  "type": "fixed",
  "name": "blob",
  "size": 4
}
""")

def fixedValues = [
    new GenericData.Fixed(fixedSchema, [1, 2, 3, 4] as byte[]),
    new GenericData.Fixed(fixedSchema, [5, 6, 7, 8] as byte[]),
]

writeAvroFile(dir, "fixed.avro", fixedSchema, fixedValues)

def enumSchema = parseSchema("""
{
  "type": "enum",
  "name": "suit",
  "symbols": ["hearts", "diamonds", "clubs", "spades"]
}
""")

def enumValues = [
    new GenericData.EnumSymbol(enumSchema, "clubs"),
    new GenericData.EnumSymbol(enumSchema, "hearts"),
    new GenericData.EnumSymbol(enumSchema, "spades"),
]

writeAvroFile(dir, "enum.avro", enumSchema, enumValues)

def recordSchema = parseSchema("""
{
  "name": "user",
  "type": "record",
  "fields": [
    { "name": "email", "type": "string" },
    { "name": "age", "type": "int" }
  ]
}
""")

def firstRecord = new GenericData.Record(recordSchema)
firstRecord.put("email", "bloblaw@example.com")
firstRecord.put("age", 42)

def secondRecord = new GenericData.Record(recordSchema)
secondRecord.put("email", "gmbluth@example.com")
secondRecord.put("age", 16)

writeAvroFile(dir, "record.avro", recordSchema, [firstRecord, secondRecord])

writeAvroFileWithCodec(
    dir,
    "string_deflate.avro",
    parseSchema('"string"'),
    ["foo", "bar", "foo"],
    CodecFactory.deflateCodec(5)
)
