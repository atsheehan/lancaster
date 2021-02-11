@Grab('org.apache.avro:avro:1.10.1')
import groovy.json.JsonOutput
import org.apache.avro.Schema
import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.generic.GenericData
import org.apache.avro.file.DataFileWriter

def dir = new File("test_cases")

if (!dir.exists() || !dir.isDirectory()) {
    println("FATAL: missing test_cases directory")
    System.exit(1)
}

def parseSchema(jsonSchema) {
    new Schema.Parser().parse(jsonSchema)
}

def writeAvroFile(dir, filename, schema, data) {
    new File(dir, filename).withOutputStream { stream ->
        def datumWriter = new GenericDatumWriter(schema)
        def dataFileWriter = new DataFileWriter(datumWriter)
        def syncMarker = "abcdefghijklmnop".getBytes()
        dataFileWriter.create(schema, stream, syncMarker)

        data.each {
            dataFileWriter.append(it)
        }

        dataFileWriter.close()
    }
}

writeAvroFile(dir, "boolean.avro", parseSchema('"boolean"'), [true, false])
writeAvroFile(dir, "long.avro", parseSchema('"long"'), [42, -100, 0, -9223372036854775808, 9223372036854775807])
writeAvroFile(dir, "string.avro", parseSchema('"string"'), ["foo", "bar", "", "\u263A"])

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
