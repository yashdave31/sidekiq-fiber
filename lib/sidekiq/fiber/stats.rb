module Sidekiq
  module Fiber
    # Writes fiber execution stats to Redis.
    # Called from the processor at key lifecycle points:
    #   - fiber starts      → record in-flight fiber
    #   - fiber completes   → remove in-flight, increment completed counter
    #   - semaphore changes → update utilization
    #
    # All keys are namespaced under "sidekiq-fiber:" and expire automatically
    # so stale data doesn't accumulate after a worker restarts.
    class Stats
      NAMESPACE      = "sidekiq-fiber"
      FIBER_TTL      = 600  # 10 minutes — long-running fiber alert threshold
      THREAD_TTL     = 120  # 2 minutes — thread stats expire if worker dies
      GLOBAL_TTL     = 120

      def initialize(redis_pool)
        @redis = redis_pool
      end

      # Called when a fiber starts executing a job.
      def fiber_started(jid:, job_class:, thread_id:)
        safe_redis do |conn|
          key = fiber_key(jid)
          conn.hset(key,
            "job_class",  job_class,
            "thread_id",  thread_id,
            "started_at", Time.now.to_f
          )
          conn.expire(key, FIBER_TTL)
        end
      end

      def fiber_completed(jid:, thread_id:)
        safe_redis do |conn|
          conn.del(fiber_key(jid))
          conn.hincrby(thread_key(thread_id), "completed_total", 1)
          conn.hset(thread_key(thread_id), "last_completed_at", Time.now.to_f)
          conn.expire(thread_key(thread_id), THREAD_TTL)
        end
      end

      def update_thread_stats(thread_id:, semaphore_size:, semaphore_acquired:)
        safe_redis do |conn|
          conn.hset(thread_key(thread_id),
            "semaphore_size",     semaphore_size,
            "semaphore_acquired", semaphore_acquired
          )
          conn.expire(thread_key(thread_id), THREAD_TTL)
        end
      end

      def register_thread(thread_id:, fiber_concurrency:)
        safe_redis do |conn|
          conn.sadd(threads_index_key, thread_id)
          conn.expire(threads_index_key, GLOBAL_TTL)
          conn.hset(thread_key(thread_id),
            "semaphore_size",     fiber_concurrency,
            "semaphore_acquired", 0,
            "completed_total",    0,
            "last_completed_at",  ""
          )
          conn.expire(thread_key(thread_id), THREAD_TTL)
        end
      end

      def deregister_thread(thread_id:)
        safe_redis do |conn|
          conn.srem(threads_index_key, thread_id)
          conn.del(thread_key(thread_id))
        end
      end

      # ── Readers (used by Web UI) ─────────────────────────────────────────────

      def all_thread_stats
        @redis.with do |conn|
          thread_ids = conn.smembers(threads_index_key)
          thread_ids.filter_map do |tid|
            stats = conn.hgetall(thread_key(tid))
            next if stats.empty?
            stats.merge("thread_id" => tid)
          end
        end
      end

      def all_in_flight_fibers
        @redis.with do |conn|
          keys = conn.keys("#{NAMESPACE}:fiber:*")
          keys.filter_map do |key|
            data = conn.hgetall(key)
            next if data.empty?
            jid = key.split(":").last
            data.merge(
              "jid"        => jid,
              "running_for" => (Time.now.to_f - data["started_at"].to_f).round(1)
            )
          end
        end
      end

      def global_summary
        @redis.with do |conn|
          thread_ids   = conn.smembers(threads_index_key)
          total_active = 0
          total_max    = 0

          thread_ids.each do |tid|
            stats        = conn.hgetall(thread_key(tid))
            total_active += stats["semaphore_acquired"].to_i
            total_max    += stats["semaphore_size"].to_i
          end

          {
            thread_count:  thread_ids.size,
            total_active:  total_active,
            total_max:     total_max
          }
        end
      end

      private

      def safe_redis(&block)
        @redis.with(&block)
      rescue ConnectionPool::TimeoutError, StandardError
        # Stats writes are best-effort. A timeout or Redis blip should never
        # propagate into the fiber and fail the job.
      end

      def fiber_key(jid)        = "#{NAMESPACE}:fiber:#{jid}"
      def thread_key(thread_id) = "#{NAMESPACE}:thread:#{thread_id}"
      def threads_index_key     = "#{NAMESPACE}:threads"
    end
  end
end
