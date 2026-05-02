require_relative "lib/sidekiq/fiber/version"

Gem::Specification.new do |spec|
  spec.name          = "sidekiq-fiber"
  spec.version       = Sidekiq::Fiber::VERSION
  spec.authors       = ["Yash Dave"]
  spec.email         = ["yash@skima.ai"]
  spec.summary       = "Fiber-based concurrency for Sidekiq IO-bound jobs"
  spec.description   = <<~DESC
    sidekiq-fiber lets you run IO-bound Sidekiq jobs as fibers instead of threads.
    A single thread can process thousands of concurrent jobs that spend most of
    their time waiting on external IO (HTTP, LLM APIs, S3) — without the memory
    and OS overhead of one thread per job.
  DESC
  spec.homepage      = "https://github.com/yashdave00/sidekiq-fiber"
  spec.license       = "MIT"
  spec.required_ruby_version = ">= 3.0.0"

  spec.files         = Dir["lib/**/*.rb", "LICENSE", "README.md"]

  spec.add_dependency "sidekiq", ">= 7.0"
  spec.add_dependency "async", ">= 2.0"

  spec.add_development_dependency "rspec", "~> 3.0"
end
