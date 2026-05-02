require "sidekiq/manager"

module Sidekiq
  module Fiber
    # Patches Sidekiq::Manager to respect a :processor_class config key
    # on a capsule, mirroring how Sidekiq already supports :fetch_class.
    #
    # Without this patch, Manager hardcodes Sidekiq::Processor for every
    # capsule. With it, a fiber capsule can declare its own processor:
    #
    #   config.capsule("fiber") do |cap|
    #     cap[:processor_class] = Sidekiq::Fiber::Processor
    #   end
    module ManagerPatch
      def initialize(capsule)
        super
        @processor_class = capsule.config[:processor_class] || Sidekiq::Processor
        # Rebuild @workers using the correct processor class.
        # super already populated @workers with Sidekiq::Processor instances
        # so we replace them here.
        @workers.clear
        @count.times do
          @workers << @processor_class.new(@config, &method(:processor_result))
        end
      end

      def processor_result(processor, reason = nil)
        @plock.synchronize do
          @workers.delete(processor)
          unless @done
            p = @processor_class.new(@config, &method(:processor_result))
            @workers << p
            p.start
          end
        end
      end
    end
  end
end

Sidekiq::Manager.prepend(Sidekiq::Fiber::ManagerPatch)
