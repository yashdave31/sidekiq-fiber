# sidekiq-fiber 


Fiber-based concurrency for IO-bound Sidekiq jobs.

If your jobs mostly wait on network calls — LLM APIs, HTTP requests, S3 — threads are the wrong tool. A thread blocks completely during IO. With 20 threads and 1500 queued jobs, you're processing 20 at a time and the rest sit idle.

This gem helps you solve this by using Ruby Fibers which take up a few kb per fiber and can help process 100s of fibers per thread. 

## How it works

Normal Sidekiq:
```
Thread → fetch job → perform (blocks on IO) → fetch next
```

With sidekiq-fiber:
```
Thread → fetch job 1 → schedule as fiber → hits IO, suspends
       → fetch job 2 → schedule as fiber → hits IO, suspends
       → fetch job 3 → schedule as fiber → running
       → job 1 resumes (IO done) → completes → fetch job 4
```

One thread. Many concurrent fibers. No blocking.

## Requirements

- Ruby 3.2+
- Sidekiq 7.0+
- Jobs must use fiber-aware IO clients (see table below)

## Installation

```ruby
gem "sidekiq-fiber"
```

## Usage

**1. Configure a dedicated fiber capsule**

```ruby
# config/initializers/sidekiq.rb
require "sidekiq-fiber"

Sidekiq.configure_server do |config|
  config[:fiber_concurrency] = 50  # max concurrent fibers per thread

  config.capsule("fiber") do |cap|
    cap.concurrency = 20           # threads
    cap.queues      = ["llm_jobs", "api_calls"]
    cap.processor_class = Sidekiq::Fiber::Processor
  end
end
```

**2. Opt in per job**

```ruby
class MyLlmJob
  include Sidekiq::Worker
  include Sidekiq::Fiber::Worker  # opt in — you are responsible for fiber-safe IO

  def perform(id)
    # Net::HTTP is fiber-aware in Ruby 3.2+ — yields during socket wait
    response = Net::HTTP.get(URI("https://api.openai.com/..."))
    process(response)
  end
end
```

**3. Add the Web UI tab (optional)**

```ruby
# config/routes.rb
require "sidekiq/fiber/web"

Sidekiq::Web.configure do |config|
  config.register Sidekiq::Fiber::Web,
    name:     "sidekiq-fiber",
    tab:      "Fibers",
    index:    "fiber-stats",
    root_dir: Gem.loaded_specs["sidekiq-fiber"].gem_dir
end
```

## Fiber-safe IO

You're responsible for using fiber-aware clients. Blocking IO freezes the whole thread.

| Client | Fiber-aware in Ruby 3.2? |
|---|---|
| `Net::HTTP` | ✅ Yes — via fiber scheduler |
| `Faraday` (net-http adapter) | ✅ Yes |
| `HTTParty` | ✅ Yes (uses Net::HTTP) |
| `ActiveRecord` queries | ✅ Yes — with our connection pool patch |
| `Typhoeus` | ❌ No — uses libcurl |

### ActiveRecord connection pool

sidekiq-fiber patches `ActiveRecord::ConnectionPool` to use `Fiber.current` as the connection cache key instead of `Thread.current`. Each fiber gets its own connection slot.

Keep this in mind when sizing your DB pool:

```
max DB connections = threads × fiber_concurrency
```

If your DB pool is 100 and you have 20 threads, set `fiber_concurrency` to 5 or lower. The Web UI will warn you when you're over this limit

## Capacity planning

```
max concurrent fibers  = capsule.concurrency × fiber_concurrency
max DB connections     = capsule.concurrency × fiber_concurrency
peak memory (HTTP)     ≈ max concurrent fibers × ~50KB per connection
```

## Benchmark

Measured on Ruby 3.2.2, MacBook Air M2.
**Thread model:** 20 threads, one job at a time (max 20 concurrent)
**Fiber model:** 20 threads × 50 fibers (max 1000 concurrent)

### Part 1 — sleep() simulation (fixed latency)

`sleep()` is fiber-scheduler aware in Ruby 3.2. In a thread it blocks completely. In a fiber it yields, so other fibers can run. Results are clean and predictable here.

| Scenario | Jobs | IO wait | Threads | Fibers | Speedup |
|---|---|---|---|---|---|
| Light | 50 | 0.1s | 0.32s | 0.10s | **3.0x** |
| Medium | 200 | 0.5s | 5.04s | 0.51s | **9.9x** |
| Heavy | 500 | 1.0s | 25.10s | 1.02s | **24.7x** |

### Part 2 — Real HTTP calls (variable latency)

Jobs make actual `Net::HTTP` requests to a local WEBrick server that responds after a random delay. Real socket IO. `Net::HTTP` in Ruby 3.2 hooks into the fiber scheduler — the socket read suspends the fiber and resumes it when data arrives.

| Scenario | Jobs | Latency range | Threads | Fibers | Speedup |
|---|---|---|---|---|---|
| Fast API | 50 | 0.05s–0.3s | 0.63s | 0.31s | **2.1x** |
| Medium API | 100 | 0.5s–2.0s | 7.64s | 2.06s | **3.7x** |
| LLM-like | 50 | 2.0s–8.0s | 16.40s | 7.84s | **2.1x** |
| **Real burst** | **1500** | **0.5s–2.0s** | **95.46s** | **17.44s** | **5.5x** |

### Inference

**Speedup scales with job_count / thread_count.** The more jobs you have relative to threads, the more fibers help. The Heavy scenario hits 24.7x because 500 jobs / 20 threads = 25 serial batches — fibers collapse that to 1. (Best usecase)

**The LLM-like scenario (50 jobs, 2-8s) is only 2.1x** because with 50 jobs and 1000 fiber slots, all 50 start immediately. Both approaches then wait for the slowest request (~8s). Fibers don't make requests faster — they stop threads from sitting idle during IO.

**The Real burst scenario (1500 jobs, 0.5-2.0s) is the honest one** — closest to high traffic production LLM workloads. Threads: 95s. Fibers: 17s. Threads process 20 at a time across 75 serial batches. Fibers process up to 1000 across 2 batches.

**Memory:** the Real burst run peaked at +52.9MB for fibers vs +3.3MB for threads. 1000 concurrent HTTP connections means 1000 open sockets with buffers and response objects in memory. Set `fiber_concurrency` based on your memory budget, not just throughput.

To reproduce:

```bash
bundle exec ruby bench/throughput.rb
```

## Web UI

The optional "Fibers" tab shows:

- **Global health** — active fibers, semaphore utilization, DB connection warning
- **Per-thread breakdown** — semaphore fill, throughput, blocking IO detection
- **In-flight fibers** — long-running fiber alerts (> 30s)

**Blocking IO detection:** if a thread has active fibers but zero completions for > 60 seconds, the UI flags it. This catches jobs accidentally using non-fiber-aware clients that freeze the whole thread.

## Design decisions

**Why a dedicated capsule, not mixed with normal jobs?**
Fiber and non-fiber jobs on the same processor share thread-level interrupt handling and connection pool state in ways that are subtle to reason about. A dedicated capsule makes the boundary structural and hence avoid accidental issues.

**Why the `async` gem for the event loop?**
The fiber scheduler interface in Ruby 3.2 has edge cases around IO readiness, timeout handling, and signal interrupts. `async` is battle tested and I didn't want to reinvent the wheel for this.

**Why patch `connection_cache_key` instead of a custom pool?**
The patch is 3 lines and uses the pool's designed extension point. A custom pool would duplicate hundreds of lines of connection management logic and diverge from ActiveRecord's battle-tested implementation. 

**Why opt-in per job instead of per queue?**
Writing fiber-safe code requires deliberate choices about which IO clients you use. Making it explicit at the job class level enforces that — a queue declaration hides it. If you use a blocking client inside a fiber job, the whole thread stalls silently. Explicit opt-in helps the developers make accidental mistakes.

## License

MIT
