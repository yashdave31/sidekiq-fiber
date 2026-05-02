#!/usr/bin/env bash
# Enqueues demo jobs. Pass a count as first arg (default 30).
# Run from the demo/ directory.
set -e
export PATH=~/.rvm/rubies/ruby-3.2.2/bin:~/.rvm/gems/ruby-3.2.2/bin:$PATH

exec bundle exec ruby enqueue.rb "$@"
