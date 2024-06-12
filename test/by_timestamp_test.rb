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
    File.readlines(LOG_FILE).map(&:strip).reject { |sql| sql.include?('information_schema.columns') }.uniq
  end

  def test_empty
    @helper.src_sql('CREATE TABLE test1 (id serial PRIMARY KEY, name VARCHAR(50), created_at TIMESTAMP NOT NULL);')
    @helper.target_sql('CREATE TABLE test1 (id serial PRIMARY KEY, name VARCHAR(50), created_at TIMESTAMP NOT NULL);')

    assert @helper.run_diff(OPTIONS)

    assert_equal [
      'SELECT min(created_at) as k FROM test1',
      'SELECT max(created_at) as k FROM test1',
      'select id, name, created_at from test1 WHERE 1 = 1 ORDER BY id'
    ], sql_commands
  end

  def test_with_two_lines
    now = Time.now
    @helper.src_sql('CREATE TABLE test1 (id serial PRIMARY KEY, name VARCHAR(50), created_at TIMESTAMP NOT NULL);')
    @helper.src_sql('INSERT INTO test1 VALUES (1, \'a\', $1), (200, \'b\', $2);', [now, now + (4 * 3600 * 24)])
    @helper.target_sql('CREATE TABLE test1 (id serial PRIMARY KEY, name VARCHAR(50), created_at TIMESTAMP NOT NULL);')
    @helper.target_sql('INSERT INTO test1 VALUES (1, \'a\', $1), (200, \'b\', $2);', [now, now + (4 * 3600 * 24)])

    assert @helper.run_diff(OPTIONS)

    now_str = now.strftime('%Y-%m-%d %H:%M:%S %z')
    now_stop_str = (now + (10 * 3600 * 24)).strftime('%Y-%m-%d %H:%M:%S %z')

    assert_equal [
      'SELECT min(created_at) as k FROM test1',
      'SELECT max(created_at) as k FROM test1',
      "select id, name, created_at from test1 WHERE created_at >= '#{now_str}' AND created_at < '#{now_stop_str}' ORDER BY id" # rubocop:disable Layout/LineLength
    ], sql_commands
  end

  def test_with_two_lines_key_start_stop # rubocop:disable Minitest/MultipleAssertions
    now = Time.now
    @helper.src_sql('CREATE TABLE test1 (id serial PRIMARY KEY, name VARCHAR(50), created_at TIMESTAMP NOT NULL);')
    @helper.src_sql('INSERT INTO test1 VALUES (1, \'a\', $1), (200, \'b\', $2);', [now, now + (40 * 3600 * 24)])
    @helper.target_sql('CREATE TABLE test1 (id serial PRIMARY KEY, name VARCHAR(50), created_at TIMESTAMP NOT NULL);')
    @helper.target_sql('INSERT INTO test1 VALUES (1, \'a\', $1), (200, \'c\', $2);', [now, now + (40 * 3600 * 24)])

    refute @helper.run_diff(OPTIONS)

    FileUtils.rm_f(LOG_FILE)

    assert @helper.run_diff(OPTIONS + " --key_stop='#{now + (5 * 3600 * 24)}'")

    now_str = now.strftime('%Y-%m-%d %H:%M:%S %z')
    now_stop_str = (now + (5 * 3600 * 24)).strftime('%Y-%m-%d %H:%M:%S %z')

    assert_equal sql_commands, [
      'SELECT min(created_at) as k FROM test1',
      "select id, name, created_at from test1 WHERE created_at >= '#{now_str}' AND created_at < '#{now_stop_str}' ORDER BY id" # rubocop:disable Layout/LineLength
    ]
    FileUtils.rm_f(LOG_FILE)

    assert @helper.run_diff(OPTIONS + " --key_start='#{now + (1 * 3600 * 24)}' --key_stop='#{now + (5 * 3600 * 24)}'")

    now_str = (now + (1 * 3600 * 24)).strftime('%Y-%m-%d %H:%M:%S %z')
    now_stop_str = (now + (5 * 3600 * 24)).strftime('%Y-%m-%d %H:%M:%S %z')

    assert_equal sql_commands, [
      "select id, name, created_at from test1 WHERE created_at >= '#{now_str}' AND created_at < '#{now_stop_str}' ORDER BY id" # rubocop:disable Layout/LineLength
    ]
  end

  def test_with_relative_key_stop
    now = Time.now
    @helper.src_sql('CREATE TABLE test1 (id serial PRIMARY KEY, name VARCHAR(50), created_at TIMESTAMP NOT NULL);')
    @helper.src_sql('INSERT INTO test1 VALUES (1, \'a\', $1), (200, \'b\', $2);', [now - 10, now - 2])
    @helper.target_sql('CREATE TABLE test1 (id serial PRIMARY KEY, name VARCHAR(50), created_at TIMESTAMP NOT NULL);')
    @helper.target_sql('INSERT INTO test1 VALUES (1, \'a\', $1), (200, \'c\', $2);', [now - 10, now - 2])

    refute @helper.run_diff(OPTIONS)

    FileUtils.rm_f(LOG_FILE)

    @helper.run_diff("#{OPTIONS} --key_stop='now-5'")

    now_str = (now - 10).strftime('%Y-%m-%d %H:%M:%S %z')
    now_stop_str = (now - 5).strftime('%Y-%m-%d %H:%M:%S %z')
    now_stop_str_plus1 = (now - 4).strftime('%Y-%m-%d %H:%M:%S %z')

    potential_sql_commands =
      [now_stop_str, now_stop_str_plus1].map do |x|
        "select id, name, created_at from test1 WHERE created_at >= '#{now_str}' AND created_at < '#{x}' ORDER BY id"
      end

    assert_includes potential_sql_commands, sql_commands[1]
  end
end
