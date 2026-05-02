require_relative "jobs"  # load job classes before configure_server runs

Sidekiq.configure_server do |config|
  config.redis = { url: "redis://localhost:6379/0" }

  # fiber_concurrency controls max concurrent fibers per thread
  config[:fiber_concurrency] = 20

  # dedicated fiber capsule — separate from normal jobs
  config.capsule("fiber") do |cap|
    cap.concurrency = 2           # 2 threads × 20 fibers = 40 concurrent fiber jobs
    cap.queues      = ["fiber"]
    cap.processor_class = Sidekiq::Fiber::Processor
  end
end

Sidekiq.configure_client do |config|
  config.redis = { url: "redis://localhost:6379/0" }
end
