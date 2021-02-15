Gem::Specification.new do |s|
  s.name        = 'lancaster'
  s.version     = '0.0.0'
  s.date        = '2021-01-26'
  s.summary     = "Rust avro parsing"
  s.description = "An Avro reader"
  s.authors     = ["Adam Sheehan"]
  s.email       = 'asheehan@salsify.com'
  s.files       = ["lib/lancaster.rb"]
  s.license       = 'MIT'

  s.add_dependency 'rutie', '~> 0.0.3'
  s.add_development_dependency 'avro'
  s.add_development_dependency 'benchmark-ips'
  s.add_development_dependency 'rake'
end
