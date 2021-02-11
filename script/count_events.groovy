@Grab('org.apache.avro:avro:1.10.1')
import groovy.json.JsonOutput
import org.apache.avro.Schema
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.generic.GenericRecord
import org.apache.avro.file.DataFileReader

if (args.length < 1) {
    println("FATAL: please specify an Avro file to count");
    System.exit(1)
}

def file = new File(args[0])

def datumReader = new GenericDatumReader<GenericRecord>();
def dataFileReader = new DataFileReader<GenericRecord>(file, datumReader);

def count = 0

def user = null

while (dataFileReader.hasNext()) {
    user = dataFileReader.next(user)
    count += 1
}

println("count: " + count)
