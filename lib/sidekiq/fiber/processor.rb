require "async"
require "async/semaphore"
require "sidekiq/processor"

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
        @fiber_concurrency = capsule.config[:fiber_concurrency] || 100
      end

      private

      # Replaces the default run loop. Instead of processing one job at a time,
      # we start an Async event loop and schedule each fiber job as a child task.
      # The semaphore prevents unbounded fiber growth.
      def run
        Thread.current[:sidekiq_capsule] = @capsule

        semaphore = Async::Semaphore.new(@fiber_concurrency)

        Async do |task|
          until @done
            uow = fetch
            next unless uow

            klass_name = begin
              Sidekiq.load_json(uow.job)["class"]
            rescue
              nil
            end

            is_fiber_job = klass_name &&
              Object.const_get(klass_name).include?(Sidekiq::Fiber::Worker)

            if is_fiber_job
              task.async do
                semaphore.acquire do
                  process_in_fiber(uow)
                end
              end
            else
              # Non-fiber job: run inline on this thread as normal.
              # This should not happen if the capsule is correctly configured
              # to only receive fiber jobs — but we handle it safely.
              process(uow)
            end
          end
          # Async waits here for all child tasks to finish before returning.
        end

        @callback.call(self)
      rescue Sidekiq::Shutdown
        @callback.call(self)
      rescue Exception => ex
        @callback.call(self, ex)
      end

      # Runs a single job unit of work inside the current fiber.
      # Mirrors Sidekiq::Processor#process but with per-fiber ack tracking
      # so that hard shutdown can requeue incomplete jobs correctly.
      def process_in_fiber(uow)
        jobstr   = uow.job
        queue    = uow.queue_name
        job_hash = nil

        begin
          job_hash = Sidekiq.load_json(jobstr)
        rescue => ex
          handle_exception(ex, { context: "Invalid JSON for job", jobstr: jobstr })
          return uow.acknowledge
        end

        ack = false

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
          uow.acknowledge if ack
        end
      end
    end
  end
end
