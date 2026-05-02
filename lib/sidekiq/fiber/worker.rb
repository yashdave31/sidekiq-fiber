module Sidekiq
  module Fiber
    # Marker module. Including this in a Sidekiq job class signals to
    # SidekiqFiber::Processor that this job should run as a fiber.
    #
    # The developer is responsible for ensuring all IO inside the job
    # uses fiber-aware clients. Blocking IO (e.g. a non-patched HTTP
    # client) will block the entire thread, defeating the purpose.
    #
    # Example:
    #
    #   class MyJob
    #     include Sidekiq::Worker
    #     include Sidekiq::Fiber::Worker
    #
    #     def perform(id)
    #       # only fiber-aware IO here
    #       Net::HTTP.get(URI("https://api.example.com/#{id}"))
    #     end
    #   end
    module Worker
    end
  end
end
