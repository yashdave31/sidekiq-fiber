require "sidekiq"
require "async"

require_relative "sidekiq/fiber/version"
require_relative "sidekiq/fiber/worker"
require_relative "sidekiq/fiber/processor"
require_relative "sidekiq/fiber/connection_pool_patch"
require_relative "sidekiq/fiber/manager_patch"
