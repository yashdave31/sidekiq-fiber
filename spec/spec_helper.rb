require "sidekiq-fiber"
require "async/semaphore"
require "sidekiq/testing"

Sidekiq::Testing.fake!

RSpec.configure do |config|
  config.expect_with :rspec do |c|
    c.syntax = :expect
  end
end
