namespace :benchmark do
  desc 'Run benchmarks for decoding Avro datafiles'
  task :decode_file do
    require_relative 'benchmark/decoding'
    DecodingBenchmark.run
  end
end
