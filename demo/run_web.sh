#!/usr/bin/env bash
# Boots the Sidekiq Web UI on http://localhost:9292
# Run from the demo/ directory.
set -e
export PATH=~/.rvm/rubies/ruby-3.2.2/bin:~/.rvm/gems/ruby-3.2.2/bin:$PATH

exec bundle exec puma config.ru -p 9292
