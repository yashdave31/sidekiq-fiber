require "spec_helper"

RSpec.describe Sidekiq::Fiber::Processor do
  let(:config) do
    Sidekiq::Config.new.tap do |c|
      c[:concurrency] = 1
      c[:fiber_concurrency] = 10
    end
  end

  let(:capsule) { Sidekiq::Capsule.new("fiber", config) }

  # Minimal unit of work double that tracks whether it was acknowledged
  def build_uow(job_class_name, args: [])
    job_hash = { "class" => job_class_name, "args" => args, "jid" => SecureRandom.hex(12) }
    double("uow",
      job: JSON.generate(job_hash),
      queue_name: "fiber_test",
      acknowledge: true,
      requeue: true
    )
  end

  describe "fiber job detection" do
    let(:fiber_job) do
      stub_const("FiberTestJob", Class.new do
        include Sidekiq::Job
        include Sidekiq::Fiber::Worker
        def perform; end
      end)
      "FiberTestJob"
    end

    let(:normal_job) do
      stub_const("NormalTestJob", Class.new do
        include Sidekiq::Job
        def perform; end
      end)
      "NormalTestJob"
    end

    it "identifies fiber jobs correctly" do
      klass = Object.const_get(fiber_job)
      expect(klass.include?(Sidekiq::Fiber::Worker)).to be true
    end

    it "identifies normal jobs correctly" do
      klass = Object.const_get(normal_job)
      expect(klass.include?(Sidekiq::Fiber::Worker)).to be false
    end
  end

  describe "semaphore" do
    it "initialises with fiber_concurrency from config" do
      processor = Sidekiq::Fiber::Processor.new(capsule) {}
      # fiber_concurrency is 10 in our config
      expect(processor.instance_variable_get(:@fiber_concurrency)).to eq(10)
    end

    it "defaults to 100 when fiber_concurrency is not configured" do
      bare_config = Sidekiq::Config.new.tap { |c| c[:concurrency] = 1 }
      bare_capsule = Sidekiq::Capsule.new("fiber", bare_config)
      processor = Sidekiq::Fiber::Processor.new(bare_capsule) {}
      expect(processor.instance_variable_get(:@fiber_concurrency)).to eq(100)
    end
  end

  describe "job acknowledgement" do
    it "acknowledges a job that completes successfully" do
      stub_const("AckTestJob", Class.new do
        include Sidekiq::Job
        include Sidekiq::Fiber::Worker
        def perform; end
      end)

      uow = build_uow("AckTestJob")
      processor = Sidekiq::Fiber::Processor.new(capsule) {}

      expect(uow).to receive(:acknowledge)
      processor.send(:process_in_fiber, uow)
    end

    it "does not acknowledge a job that raises before completion" do
      stub_const("FailingTestJob", Class.new do
        include Sidekiq::Job
        include Sidekiq::Fiber::Worker
        def perform
          raise "boom"
        end
      end)

      uow = build_uow("FailingTestJob")
      processor = Sidekiq::Fiber::Processor.new(capsule) {}

      # Bypass the full retry middleware chain so we see the raw ack behaviour.
      # The retry middleware catches errors and re-raises as Handled, which sets
      # ack = true. Here we test our processor's own ack logic directly.
      allow(processor).to receive(:dispatch).and_yield(FailingTestJob.new)

      expect(uow).not_to receive(:acknowledge)
      expect { processor.send(:process_in_fiber, uow) }.to raise_error(RuntimeError, "boom")
    end
  end

  describe "concurrency" do
    it "runs multiple fiber jobs concurrently within the semaphore limit" do
      execution_order = []

      # Use a closure over execution_order rather than a class variable
      # (class variables are forbidden in anonymous classes at toplevel in Ruby)
      order_ref = execution_order

      stub_const("ConcurrentTestJob", Class.new do
        include Sidekiq::Job
        include Sidekiq::Fiber::Worker

        define_method(:perform) do |id|
          order_ref << "start-#{id}"
          Async::Task.current.sleep(0.01)
          order_ref << "end-#{id}"
        end
      end)

      uows = (1..3).map { |i| build_uow("ConcurrentTestJob", args: [i]) }
      processor = Sidekiq::Fiber::Processor.new(capsule) {}

      Async do |task|
        semaphore = Async::Semaphore.new(10)

        tasks = uows.map do |uow|
          task.async do
            semaphore.acquire { processor.send(:process_in_fiber, uow) }
          end
        end

        tasks.each(&:wait)
      end

      starts = execution_order.select { |e| e.start_with?("start") }
      ends   = execution_order.select { |e| e.start_with?("end") }

      expect(starts.length).to eq(3)
      expect(ends.length).to eq(3)
      # All three jobs started before any ended — proving concurrent execution
      expect(execution_order.index("start-3")).to be < execution_order.index("end-1")
    end
  end
end
