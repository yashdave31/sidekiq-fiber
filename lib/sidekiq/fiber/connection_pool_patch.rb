module Sidekiq
  module Fiber
    # Patches ActiveRecord's connection pool to use Fiber.current as the
    # cache key instead of Thread.current.
    #
    # Without this patch, all fibers on the same thread share one connection
    # slot — causing either connection sharing (data corruption) or unbounded
    # connection checkout (one per fiber).
    #
    # With this patch, each fiber gets its own connection slot. The developer
    # must still bound fiber concurrency via fiber_concurrency config to avoid
    # exhausting the database connection limit.
    #
    # Only applied when ActiveRecord is present.
    module ConnectionPoolPatch
      def connection_cache_key(_thread)
        ::Fiber.current
      end
    end
  end
end

if defined?(ActiveRecord::ConnectionAdapters::ConnectionPool)
  ActiveRecord::ConnectionAdapters::ConnectionPool.prepend(
    Sidekiq::Fiber::ConnectionPoolPatch
  )
end
