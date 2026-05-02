require "sidekiq/manager"
require "sidekiq/capsule"

module Sidekiq
  module Fiber
    # Extends Sidekiq::Capsule with a per-capsule processor_class attribute.
    # We can't use capsule[:processor_class] because capsule delegates [] to
    # the global config — setting it would affect all capsules.
    module CapsulePatch
      def processor_class=(klass)
        @processor_class = klass
      end

      def processor_class
        @processor_class
      end
    end

    # Patches Sidekiq::Manager to respect the per-capsule processor_class.
    module ManagerPatch
      def initialize(capsule)
        super
        klass = capsule.respond_to?(:processor_class) && capsule.processor_class
        @processor_class = klass || Sidekiq::Processor
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

Sidekiq::Capsule.prepend(Sidekiq::Fiber::CapsulePatch)
Sidekiq::Manager.prepend(Sidekiq::Fiber::ManagerPatch)
