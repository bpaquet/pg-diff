# frozen_string_literal: true

require 'minitest/autorun'

require_relative 'helper'
class ByTimestampTest < Minitest::Test
  LOG_FILE = '/tmp/sql.log'
  OPTIONS = '--tables test1 --strategy=by_timestamp --key=created_at ' \
            "--batch_size=10 --record_sql_file #{LOG_FILE}".freeze

  def setup
    FileUtils.rm_f(LOG_FILE)
    @helper = Helper.new
    @helper.src_sql('DROP TABLE IF EXISTS test1;')
    @helper.target_sql('DROP TABLE IF EXISTS test1;')
  end

  def sql_commands
    File.readlines(LOG_FILE).map(&:strip).reject { |sql| sql.include?('information_schema.columns') }
  end

  def test_empty
    @helper.src_sql('CREATE TABLE test1 (id serial PRIMARY KEY, name VARCHAR(50), created_at TIMESTAMP NOT NULL);')
    @helper.target_sql('CREATE TABLE test1 (id serial PRIMARY KEY, name VARCHAR(50), created_at TIMESTAMP NOT NULL);')

    assert @helper.run_diff(OPTIONS)

    assert_equal [
      'SELECT min(created_at) as k FROM test1',
      'SELECT max(created_at) as k FROM test1',
      'select created_at, id, name from test1 WHERE 1 = 1 ORDER BY id'
    ], sql_commands.uniq
  end

  def test_with_two_lines # rubocop:disable Metrics/MethodLength
    now = DateTime.now.new_offset(0)
    @helper.src_sql('CREATE TABLE test1 (id serial PRIMARY KEY, name VARCHAR(50), created_at TIMESTAMP NOT NULL);')
    @helper.src_sql('INSERT INTO test1 VALUES (1, \'a\', $1), (200, \'b\', $2);', [now, now + 4])
    @helper.target_sql('CREATE TABLE test1 (id serial PRIMARY KEY, name VARCHAR(50), created_at TIMESTAMP NOT NULL);')
    @helper.target_sql('INSERT INTO test1 VALUES (1, \'a\', $1), (200, \'b\', $2);', [now, now + 4])

    assert @helper.run_diff(OPTIONS)

    now_str = now.strftime('%Y-%m-%dT%H:%M:%S%:z')
    now_stop_str = (now + 10).strftime('%Y-%m-%dT%H:%M:%S%:z')

    assert_equal sql_commands.uniq, [
      'SELECT min(created_at) as k FROM test1',
      'SELECT max(created_at) as k FROM test1',
      "select created_at, id, name from test1 WHERE created_at >= '#{now_str}' AND created_at < '#{now_stop_str}' ORDER BY id" # rubocop:disable Layout/LineLength
    ]
  end

  def test_with_two_lines_key_start_stop # rubocop:disable Metrics/MethodLength,Metrics/AbcSize,Minitest/MultipleAssertions
    now = DateTime.now.new_offset(0)
    @helper.src_sql('CREATE TABLE test1 (id serial PRIMARY KEY, name VARCHAR(50), created_at TIMESTAMP NOT NULL);')
    @helper.src_sql('INSERT INTO test1 VALUES (1, \'a\', $1), (200, \'b\', $2);', [now, now + 40])
    @helper.target_sql('CREATE TABLE test1 (id serial PRIMARY KEY, name VARCHAR(50), created_at TIMESTAMP NOT NULL);')
    @helper.target_sql('INSERT INTO test1 VALUES (1, \'a\', $1), (200, \'c\', $2);', [now, now + 40])

    refute @helper.run_diff(OPTIONS)

    FileUtils.rm_f(LOG_FILE)

    assert @helper.run_diff(OPTIONS + " --key_stop='#{now + 5}'")

    now_str = now.strftime('%Y-%m-%dT%H:%M:%S%:z')
    now_stop_str = (now + 10).strftime('%Y-%m-%dT%H:%M:%S%:z')

    assert_equal sql_commands.uniq, [
      'SELECT min(created_at) as k FROM test1',
      "select created_at, id, name from test1 WHERE created_at >= '#{now_str}' AND created_at < '#{now_stop_str}' ORDER BY id" # rubocop:disable Layout/LineLength
    ]
    FileUtils.rm_f(LOG_FILE)

    assert @helper.run_diff(OPTIONS + " --key_start=#{now + 1} --key_stop='#{now + 5}'")

    now_str = (now + 1).strftime('%Y-%m-%dT%H:%M:%S%:z')
    now_stop_str = (now + 11).strftime('%Y-%m-%dT%H:%M:%S%:z')

    assert_equal sql_commands.uniq, [
      "select created_at, id, name from test1 WHERE created_at >= '#{now_str}' AND created_at < '#{now_stop_str}' ORDER BY id" # rubocop:disable Layout/LineLength
    ]
  end
end
