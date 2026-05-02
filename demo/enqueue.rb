require_relative "jobs"

Sidekiq.configure_client do |config|
  config.redis = { url: "redis://localhost:6379/0" }
end

count = (ARGV[0] || "30").to_i

puts "Enqueuing #{count} FakeLlmJob(s) to the fiber queue..."
count.times do |i|
  delay = (rand * 3 + 1).round(2)   # 1–4s random delay
  FakeLlmJob.set(queue: "fiber").perform_async(i + 1, delay)
end

puts "Enqueuing 5 NormalJob(s) to the default queue..."
5.times do |i|
  NormalJob.perform_async(i + 1)
end

puts "Done."
