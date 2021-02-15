# frozen_string_literal: true

require 'avro'
require './lib/lancaster'
require 'benchmark/ips'

module DecodingBenchmark
  def self.run
    Benchmark.ips do |x|
      x.report('Pure Ruby Avro Datafile Decoding') do |times|
        times.times do
          read_datafile(Avro::DataFile, 'benchmark/record.avro')
        end
      end

      x.report('Rusted Datafile Decoding') do |times|
        times.times do
          read_datafile(DataFile, 'benchmark/record.avro')
        end
      end

      x.compare!
    end
  end

  private

  def self.read_datafile(datafile_class, avro_datafile_path)
    datafile_class.open(avro_datafile_path, 'r') do |io|
      io.each do |_record|
      end
    end
  end
end
