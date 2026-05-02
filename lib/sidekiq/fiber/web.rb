require "sidekiq/web"
require_relative "stats"

module Sidekiq
  module Fiber
    # Sidekiq Web UI extension for fiber stats.
    #
    # Registers a "Fibers" tab that shows:
    #   - Global summary (active fibers, semaphore utilization, DB connection warning)
    #   - Per-thread breakdown (semaphore fill, throughput, blocking IO detection)
    #   - In-flight fibers (long-running fiber alert)
    #
    # Usage in config/routes.rb or rack config:
    #
    #   require "sidekiq/fiber/web"
    #   Sidekiq::Web.configure do |config|
    #     config.register Sidekiq::Fiber::Web,
    #       name:     "sidekiq-fiber",
    #       tab:      "Fibers",
    #       index:    "fiber-stats",
    #       root_dir: File.expand_path("../../../..", __FILE__)
    #   end
    module Web
      VIEWS = File.expand_path("../../../web/views", __dir__)
      ROOT  = File.expand_path("../../../", __dir__)

      def self.registered(app)
        app.get "/fiber-stats" do
          pool  = Sidekiq.default_configuration.redis_pool
          stats = Sidekiq::Fiber::Stats.new(pool)

          @global         = stats.global_summary
          @thread_stats   = stats.all_thread_stats.sort_by { |t| t["thread_id"] }
          @in_flight      = stats.all_in_flight_fibers.sort_by { |f| -f["running_for"].to_f }

          # DB connection warning threshold
          @db_pool_size = begin
            ActiveRecord::Base.connection_pool.size
          rescue
            nil
          end

          render(:erb, File.read(File.join(VIEWS, "fiber_stats.html.erb")))
        end

        app.get "/fiber-stats/thread/:thread_id" do
          pool  = Sidekiq.default_configuration.redis_pool
          stats = Sidekiq::Fiber::Stats.new(pool)

          thread_id     = route_params(:thread_id)
          @thread_stats = stats.all_thread_stats.find { |t| t["thread_id"] == thread_id }
          @in_flight    = stats.all_in_flight_fibers.select { |f| f["thread_id"] == thread_id }

          render(:erb, File.read(File.join(VIEWS, "fiber_thread.html.erb")))
        end
      end
    end
  end
end
