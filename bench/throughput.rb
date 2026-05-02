require "async"
require "async/semaphore"
require "net/http"
require "webrick"
require "uri"

# ── Benchmark: Thread concurrency vs Fiber concurrency ───────────────────────
#
# Two approaches compared:
#
# 1. sleep() benchmark — simulates IO wait with fixed latency
#    sleep() is fiber-scheduler aware in Ruby 3.2:
#      - In a thread → blocks the thread for the full duration
#      - In a fiber  → yields to other fibers, resumes when done
#
# 2. Real HTTP benchmark — makes actual Net::HTTP calls to a local WEBrick
#    server that responds after a random delay (uniform between min and max).
#    This proves the gem works with real socket IO, not just sleep simulation.
#    Net::HTTP in Ruby 3.2 is fully fiber-scheduler aware.
#
# Thread model:  THREADS threads, one job at a time each. Max concurrency = THREADS.
# Fiber model:   THREADS * FIBER_CONCURRENCY concurrent fibers. Max = 1000.
#
# ─────────────────────────────────────────────────────────────────────────────

THREADS           = 20
FIBER_CONCURRENCY = 50
SERVER_PORT       = 9291

SLEEP_SCENARIOS = [
  { name: "Light",   jobs: 50,  io_wait: 0.1 },
  { name: "Medium",  jobs: 200, io_wait: 0.5 },
  { name: "Heavy",   jobs: 500, io_wait: 1.0 },
]

HTTP_SCENARIOS = [
  { name: "Fast API",   jobs: 50,   min: 0.05, max: 0.3  },
  { name: "Medium API", jobs: 100,  min: 0.5,  max: 2.0  },
  { name: "LLM-like",   jobs: 50,   min: 2.0,  max: 8.0  },
  { name: "Real burst", jobs: 1500, min: 0.5,  max: 2.0  },
]

# ── Local HTTP server ─────────────────────────────────────────────────────────

def start_server
  server = WEBrick::HTTPServer.new(
    Port:         SERVER_PORT,
    Logger:       WEBrick::Log.new(File::NULL),
    AccessLog:    []
  )

  server.mount_proc("/slow") do |req, res|
    min   = req.query["min"].to_f
    max   = req.query["max"].to_f
    delay = min + rand * (max - min)
    sleep(delay)
    res.body        = "ok"
    res.content_type = "text/plain"
  end

  Thread.new { server.start }
  sleep(0.3)  # give server time to boot
  server
end

# ── Job implementations ───────────────────────────────────────────────────────

def thread_sleep_job(io_wait)
  sleep(io_wait)
end

def fiber_sleep_job(io_wait)
  sleep(io_wait)  # fiber-aware in Ruby 3.2
end

def thread_http_job(min, max)
  Net::HTTP.get(URI("http://localhost:#{SERVER_PORT}/slow?min=#{min}&max=#{max}"))
end

def fiber_http_job(min, max)
  Net::HTTP.get(URI("http://localhost:#{SERVER_PORT}/slow?min=#{min}&max=#{max}"))
end

# ── Runners ───────────────────────────────────────────────────────────────────

def run_with_threads(job_count, &job)
  queue = Queue.new
  job_count.times { queue << true }
  done  = 0
  mu    = Mutex.new

  threads = THREADS.times.map do
    Thread.new do
      loop do
        begin
          queue.pop(true)
        rescue ThreadError
          break
        end
        job.call
        mu.synchronize do
          done += 1
          log "Threads: #{done}/#{job_count}" if done % [1, job_count / 5].max == 0
        end
      end
    end
  end

  threads.each(&:join)
end

def run_with_fibers(job_count, &job)
  max_concurrent = THREADS * FIBER_CONCURRENCY
  semaphore      = Async::Semaphore.new(max_concurrent)
  completed      = 0
  mu             = Mutex.new

  Async do |task|
    tasks = job_count.times.map do
      task.async do
        semaphore.acquire do
          job.call
          mu.synchronize do
            completed += 1
            log "Fibers:  #{completed}/#{job_count}" if completed % [1, job_count / 5].max == 0
          end
        end
      end
    end
    tasks.each(&:wait)
  end
end

# ── Helpers ───────────────────────────────────────────────────────────────────

def separator = puts("-" * 68)

def log(msg)
  puts "  [#{Time.now.strftime("%H:%M:%S")}] #{msg}"
end

def measure
  start = Process.clock_gettime(Process::CLOCK_MONOTONIC)
  yield
  Process.clock_gettime(Process::CLOCK_MONOTONIC) - start
end

def memory_mb
  (`ps -o rss= -p #{Process.pid}`.strip.to_i / 1024.0).round(1)
end

def run_scenario(label, jobs, thread_block, fiber_block)
  puts label
  separator

  mem_before_t  = memory_mb
  thread_time   = measure { run_with_threads(jobs, &thread_block) }
  mem_after_t   = memory_mb
  puts

  mem_before_f  = memory_mb
  fiber_time    = measure { run_with_fibers(jobs, &fiber_block) }
  mem_after_f   = memory_mb
  puts

  thread_tp = (jobs / thread_time).round(1)
  fiber_tp  = (jobs / fiber_time).round(1)
  speedup   = (thread_time / fiber_time).round(1)

  puts "  Threads  | Time: %6.2fs | Throughput: %6.1f jobs/s | Mem delta: %+.1fMB" % [
    thread_time, thread_tp, mem_after_t - mem_before_t
  ]
  puts "  Fibers   | Time: %6.2fs | Throughput: %6.1f jobs/s | Mem delta: %+.1fMB" % [
    fiber_time, fiber_tp, mem_after_f - mem_before_f
  ]
  puts "  Speedup  | %.1fx faster with fibers" % speedup
  puts

  { thread_time: thread_time.round(2), fiber_time: fiber_time.round(2), speedup: speedup }
end

# ── Main ──────────────────────────────────────────────────────────────────────

puts
puts "sidekiq-fiber benchmark"
puts "Ruby #{RUBY_VERSION} | Threads: #{THREADS} | Fiber concurrency: #{FIBER_CONCURRENCY}/thread"
puts "Max thread concurrency: #{THREADS} | Max fiber concurrency: #{THREADS * FIBER_CONCURRENCY}"
puts

# ── Part 1: sleep() simulation ────────────────────────────────────────────────

puts "=" * 68
puts "Part 1: sleep() simulation (fixed latency)"
puts "=" * 68
puts

sleep_results = SLEEP_SCENARIOS.map do |s|
  label = "#{s[:name]}: #{s[:jobs]} jobs × #{s[:io_wait]}s fixed IO wait"
  result = run_scenario(label, s[:jobs],
    -> { thread_sleep_job(s[:io_wait]) },
    -> { fiber_sleep_job(s[:io_wait]) }
  )
  s.merge(result)
end

# ── Part 2: real HTTP calls ───────────────────────────────────────────────────

puts "=" * 68
puts "Part 2: Real HTTP calls (variable latency via local WEBrick server)"
puts "=" * 68
puts

server = start_server
log "WEBrick server started on port #{SERVER_PORT}"
puts

http_results = HTTP_SCENARIOS.map do |s|
  label = "#{s[:name]}: #{s[:jobs]} jobs × #{s[:min]}s–#{s[:max]}s variable HTTP latency"
  result = run_scenario(label, s[:jobs],
    -> { thread_http_job(s[:min], s[:max]) },
    -> { fiber_http_job(s[:min], s[:max]) }
  )
  s.merge(result)
end

server.shutdown

# ── Summary ───────────────────────────────────────────────────────────────────

puts "=" * 68
puts "Summary — sleep() simulation"
puts "=" * 68
puts "%-14s %6s %8s %10s %10s %8s" % ["Scenario", "Jobs", "IO wait", "Threads", "Fibers", "Speedup"]
separator
sleep_results.each do |r|
  puts "%-14s %6d %7ss %9.2fs %9.2fs %7.1fx" % [
    r[:name], r[:jobs], r[:io_wait], r[:thread_time], r[:fiber_time], r[:speedup]
  ]
end

puts
puts "=" * 68
puts "Summary — real HTTP calls"
puts "=" * 68
puts "%-14s %6s %14s %10s %10s %8s" % ["Scenario", "Jobs", "Latency range", "Threads", "Fibers", "Speedup"]
separator
http_results.each do |r|
  puts "%-14s %6d %9ss–%ss %9.2fs %9.2fs %7.1fx" % [
    r[:name], r[:jobs], r[:min], r[:max], r[:thread_time], r[:fiber_time], r[:speedup]
  ]
end

puts
puts "Notes:"
puts "  - Thread model: #{THREADS} threads, one job at a time (max #{THREADS} concurrent)"
puts "  - Fiber model:  #{THREADS} threads × #{FIBER_CONCURRENCY} fibers (max #{THREADS * FIBER_CONCURRENCY} concurrent)"
puts "  - HTTP server: local WEBrick, responds after rand(min..max) seconds"
puts "  - Net::HTTP is fiber-scheduler aware in Ruby 3.2+ — real socket IO"
puts
