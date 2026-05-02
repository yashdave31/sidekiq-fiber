require "spec_helper"

RSpec.describe Sidekiq::Fiber::Worker do
  let(:fiber_job_class) do
    Class.new do
      include Sidekiq::Job
      include Sidekiq::Fiber::Worker

      def perform; end
    end
  end

  let(:normal_job_class) do
    Class.new do
      include Sidekiq::Job

      def perform; end
    end
  end

  it "marks a job class as a fiber worker" do
    expect(fiber_job_class.ancestors).to include(Sidekiq::Fiber::Worker)
  end

  it "does not affect job classes that do not include it" do
    expect(normal_job_class.ancestors).not_to include(Sidekiq::Fiber::Worker)
  end

  it "can be detected at runtime via include?" do
    expect(fiber_job_class.include?(Sidekiq::Fiber::Worker)).to be true
    expect(normal_job_class.include?(Sidekiq::Fiber::Worker)).to be false
  end
end
