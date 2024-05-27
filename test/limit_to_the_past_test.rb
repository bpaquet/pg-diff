# frozen_string_literal: true

require 'minitest/autorun'

require_relative 'helper'
class LimitToThePastTest < Minitest::Test
  LOG_FILE = '/tmp/sql.log'
  OPTIONS = '--tables test1 --strategy=by_timestamp --key=created_at ' \
            "--record_sql_file #{LOG_FILE} --batch_size=1".freeze

  def setup
    @helper = Helper.new
    @helper.src_sql('DROP TABLE IF EXISTS test1;')
    @helper.target_sql('DROP TABLE IF EXISTS test1;')
  end

  def sql_commands
    File.readlines(LOG_FILE).map(&:strip).reject { |sql| sql.include?('information_schema.columns') }.uniq
  end

  def test_limit_works
    now = Time.now
    @helper.src_sql('CREATE TABLE test1 (id serial PRIMARY KEY, name VARCHAR(50), created_at TIMESTAMP NOT NULL);')
    @helper.src_sql('INSERT INTO test1 VALUES (1, \'a\', $1), (200, \'b\', $2);', [now - (60 * 60), now - (2 * 60)])
    @helper.target_sql('CREATE TABLE test1 (id serial PRIMARY KEY, name VARCHAR(50), created_at TIMESTAMP NOT NULL);')
    @helper.target_sql('INSERT INTO test1 VALUES (1, \'a\', $1), (200, \'c\', $2);', [now - (60 * 60), now - (2 * 60)])

    refute @helper.run_diff(OPTIONS)

    FileUtils.rm_f(LOG_FILE)

    assert @helper.run_diff("#{OPTIONS} --limit_to_the_past_minutes=15")

    now_str = (now - (60 * 60)).strftime('%Y-%m-%d %H:%M:%S %z')
    now_stop_str = (now - (60 * 60) + (24 * 3600)).strftime('%Y-%m-%d %H:%M:%S %z')
    (now - (15 * 60)).strftime('%Y-%m-%d %H:%M:%S %z')
    stop_2_lower = now - (15 * 60)
    ok = false
    while stop_2_lower < now
      stop_2_str = stop_2_lower.strftime('%Y-%m-%d %H:%M:%S %z')
      if sql_commands[2] == "select id, name, created_at from test1 WHERE created_at >= '#{now_str}' " \
                            "AND created_at < '#{now_stop_str}' AND created_at < '#{stop_2_str}' ORDER BY id"
        ok = true
        break
      end
      stop_2_lower += 1
    end

    assert ok
  end
end
