require "sidekiq"
require "sidekiq/web"
require "sidekiq-fiber"
require "sidekiq/fiber/web"
require "rack/session"

Sidekiq.configure_client do |config|
  config.redis = { url: "redis://localhost:6379/0" }
end

Sidekiq::Web.configure do |config|
  config.register Sidekiq::Fiber::Web,
    name:     "sidekiq-fiber",
    tab:      "Fibers",
    index:    "fiber-stats",
    root_dir: File.expand_path("../web", __dir__)
end

use Rack::Session::Cookie,
  secret:   File.read(".session.key").strip,
  same_site: true,
  max_age:  86400

run Sidekiq::Web
