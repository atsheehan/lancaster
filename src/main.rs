use lancaster::{AvroDatafile, SchemaRegistry};
use std::env;

fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() < 2 {
        println!("specify a file");
        return;
    }

    let filename = &args[1];

    let mut schema_registry = SchemaRegistry::new();
    let datafile = AvroDatafile::open(filename, &mut schema_registry).unwrap();

    let mut count = 0;

    for _ in datafile {
        count += 1;
    }

    println!("count: {}", count);
}
