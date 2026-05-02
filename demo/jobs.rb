require "sidekiq"
require "sidekiq-fiber"
require "net/http"
require "uri"

# Simulates an LLM API call with a random delay.
# Uses sleep() which is fiber-aware in Ruby 3.2+.
class FakeLlmJob
  include Sidekiq::Job
  include Sidekiq::Fiber::Worker

  def perform(id, delay)
    puts "[fiber] job #{id} started (will sleep #{delay}s)"
    sleep(delay)
    puts "[fiber] job #{id} done"
  end
end

# A normal job with no fiber opt-in — runs inline on the thread.
class NormalJob
  include Sidekiq::Job

  def perform(id)
    puts "[thread] normal job #{id} started"
    sleep(0.5)
    puts "[thread] normal job #{id} done"
  end
end
