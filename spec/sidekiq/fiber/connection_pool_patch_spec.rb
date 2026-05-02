require "spec_helper"

RSpec.describe Sidekiq::Fiber::ConnectionPoolPatch do
  context "when ActiveRecord is not present" do
    it "does not raise on load" do
      expect { require "sidekiq-fiber" }.not_to raise_error
    end
  end

  context "when ActiveRecord is present" do
    before do
      # Simulate a minimal ActiveRecord ConnectionPool with the method we patch
      stub_const("ActiveRecord::ConnectionAdapters::ConnectionPool", Class.new do
        prepend Sidekiq::Fiber::ConnectionPoolPatch

        def connection_cache_key(thread)
          thread
        end
      end)
    end

    it "returns Fiber.current as the cache key instead of the thread" do
      pool = ActiveRecord::ConnectionAdapters::ConnectionPool.new

      current_fiber = ::Fiber.current
      expect(pool.connection_cache_key(Thread.current)).to eq(current_fiber)
    end

    it "returns different cache keys for different fibers" do
      pool = ActiveRecord::ConnectionAdapters::ConnectionPool.new

      keys = []
      f1 = ::Fiber.new { keys << pool.connection_cache_key(Thread.current) }
      f2 = ::Fiber.new { keys << pool.connection_cache_key(Thread.current) }
      f1.resume
      f2.resume

      expect(keys.size).to eq(2)
      expect(keys.uniq.size).to eq(2)
    end
  end
end
