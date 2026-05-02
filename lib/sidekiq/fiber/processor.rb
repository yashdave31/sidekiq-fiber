require "async"
require "sidekiq/processor"
require_relative "stats"

module Sidekiq
  module Fiber
    # A Sidekiq::Processor replacement that runs fiber-aware jobs as fibers
    # inside a per-thread Async event loop.
    #
    # One event loop runs per thread. Each fiber job is scheduled as a child
    # task inside that loop. The thread fetches jobs continuously, scheduling
    # each as a fiber without waiting for previous fibers to finish.
    #
    # A semaphore bounds concurrent fibers per thread. This directly controls
    # how many DB connections this processor can consume:
    #   max_connections = threads * fiber_concurrency
    #
    # On normal shutdown (@done = true), the fetch loop exits but the event
    # loop waits for all in-flight fibers to complete before returning.
    #
    # On hard shutdown (Sidekiq::Shutdown raised), in-flight fibers that have
    # not completed are requeued — identical to Sidekiq's default behaviour.
    class Processor < Sidekiq::Processor
      def initialize(capsule, &block)
        super
        @fiber_concurrency  = capsule.config[:fiber_concurrency] || 100
        @active_fibers      = 0
        @active_fibers_lock = Mutex.new
        stats_pool = capsule.config.new_redis_pool(@fiber_concurrency, "sidekiq-fiber-stats")
        @stats = Stats.new(stats_pool)
      end

      private

      # Replaces the default run loop with a fixed pool of fiber workers.
      #
      # We spawn exactly fiber_concurrency persistent fibers. Each fiber owns
      # its own fetch-process loop: it fetches one job, processes it, then
      # fetches the next. This means jobs stay in Redis until there is actual
      # capacity — we never drain the queue into memory ahead of processing.
      #
      # The previous design (single fetch loop + semaphore) fetched unboundedly
      # fast, pulling all queued jobs into pending async tasks before any fiber
      # started working. With 5000 jobs enqueued that meant 5000 tasks created
      # instantly, all invisible to the Sidekiq UI.
      def run
        Thread.current[:sidekiq_capsule] = @capsule

        thread_id = Thread.current.object_id.to_s

        @stats.register_thread(thread_id: thread_id, fiber_concurrency: @fiber_concurrency)

        Async do |task|
          workers = @fiber_concurrency.times.map do
            task.async do
              until @done
                uow = fetch
                unless uow
                  task.yield  # nothing in queue — yield so other fibers can run
                  next
                end

                klass_name = begin
                  Sidekiq.load_json(uow.job)["class"]
                rescue
                  nil
                end

                is_fiber_job = klass_name &&
                  Object.const_get(klass_name).include?(Sidekiq::Fiber::Worker)

                if is_fiber_job
                  active = @active_fibers_lock.synchronize { @active_fibers += 1 }
                  @stats.update_thread_stats(
                    thread_id:          thread_id,
                    semaphore_size:     @fiber_concurrency,
                    semaphore_acquired: active
                  )
                  process_in_fiber(uow, thread_id: thread_id)
                  active = @active_fibers_lock.synchronize { @active_fibers -= 1 }
                  @stats.update_thread_stats(
                    thread_id:          thread_id,
                    semaphore_size:     @fiber_concurrency,
                    semaphore_acquired: active
                  )
                else
                  process(uow)
                end
              end
            end
          end

          workers.each(&:wait)
        end

        @stats.deregister_thread(thread_id: thread_id)
        @callback.call(self)
      rescue Sidekiq::Shutdown
        @stats.deregister_thread(thread_id: Thread.current.object_id.to_s)
        @callback.call(self)
      rescue Exception => ex
        @stats.deregister_thread(thread_id: Thread.current.object_id.to_s)
        @callback.call(self, ex)
      end

      # Runs a single job unit of work inside the current fiber.
      # Mirrors Sidekiq::Processor#process but with per-fiber ack tracking
      # so that hard shutdown can requeue incomplete jobs correctly.
      def process_in_fiber(uow, thread_id:)
        jobstr   = uow.job
        queue    = uow.queue_name
        job_hash = nil

        begin
          job_hash = Sidekiq.load_json(jobstr)
        rescue => ex
          handle_exception(ex, { context: "Invalid JSON for job", jobstr: jobstr })
          return uow.acknowledge
        end

        jid       = job_hash["jid"]
        job_class = job_hash["class"]
        ack       = false

        @stats.fiber_started(jid: jid, job_class: job_class, thread_id: thread_id)

        begin
          dispatch(job_hash, queue, jobstr) do |instance|
            @capsule.config.server_middleware.invoke(instance, job_hash, queue) do
              execute_job(instance, job_hash["args"])
            end
          end
          ack = true
        rescue Sidekiq::Shutdown
          # Fiber was stopped before job completed. Do not acknowledge.
          # Job will be requeued by the capsule fetcher on shutdown.
        rescue Sidekiq::JobRetry::Skip => s
          ack = true
          raise s
        rescue Sidekiq::JobRetry::Handled => h
          ack = true
          e = h.cause || h
          handle_exception(e, { context: "Job raised exception", job: job_hash })
          raise e
        rescue Exception => ex
          handle_exception(ex, { context: "Internal exception!", job: job_hash, jobstr: jobstr })
          raise ex
        ensure
          @stats.fiber_completed(jid: jid, thread_id: thread_id)
          uow.acknowledge if ack
        end
      end
    end
  end
end
