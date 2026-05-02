require "spec_helper"

RSpec.describe Sidekiq::Fiber::ManagerPatch do
  let(:config) do
    Sidekiq::Config.new.tap do |c|
      c[:concurrency] = 2
    end
  end

  context "when no processor_class is set on the capsule" do
    it "uses Sidekiq::Processor by default" do
      capsule = Sidekiq::Capsule.new("default", config)
      manager = Sidekiq::Manager.new(capsule)

      expect(manager.workers).to all(be_a(Sidekiq::Processor))
    end
  end

  context "when processor_class is set to Sidekiq::Fiber::Processor" do
    it "instantiates the custom processor class" do
      capsule = Sidekiq::Capsule.new("fiber", config)
      capsule[:processor_class] = Sidekiq::Fiber::Processor

      manager = Sidekiq::Manager.new(capsule)

      expect(manager.workers).to all(be_a(Sidekiq::Fiber::Processor))
    end

    it "creates the correct number of processors" do
      capsule = Sidekiq::Capsule.new("fiber", config)
      capsule[:processor_class] = Sidekiq::Fiber::Processor
      capsule.concurrency = 3

      manager = Sidekiq::Manager.new(capsule)

      expect(manager.workers.size).to eq(3)
    end
  end
end
