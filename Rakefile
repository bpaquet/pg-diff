# frozen_string_literal: true

require 'rubocop/rake_task'
require 'minitest/test_task'

task default: :test

RuboCop::RakeTask.new do |task|
  task.requires << 'rubocop-rake'
end

Minitest::TestTask.create(:test) do |t|
  t.test_globs = ['test/**/*_test.rb']
end
