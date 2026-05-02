#!/usr/bin/env bash
# Boots the Sidekiq worker using Ruby 3.2.2 from RVM.
# Run from the demo/ directory.
set -e
export PATH=~/.rvm/rubies/ruby-3.2.2/bin:~/.rvm/gems/ruby-3.2.2/bin:$PATH

exec bundle exec sidekiq \
  -r ./worker.rb \
  -C sidekiq.yml
