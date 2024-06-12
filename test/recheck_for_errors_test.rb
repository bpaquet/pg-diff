# frozen_string_literal: true

require 'minitest/autorun'

require_relative 'helper'
class ByIdTest < Minitest::Test
  LOG_FILE = '/tmp/sql.log'
  OPTIONS = "--tables test1 --strategy=by_id --record_sql_file #{LOG_FILE} --recheck_for_errors=1".freeze

  def setup
    FileUtils.rm_f(LOG_FILE)
    @helper = Helper.new
    @helper.src_sql('DROP TABLE IF EXISTS test1;')
    @helper.target_sql('DROP TABLE IF EXISTS test1;')
  end

  def sql_commands
    File.readlines(LOG_FILE).map(&:strip).reject { |sql| sql.include?('information_schema.columns') }
  end

  def test_with_two_lines
    @helper.src_sql('CREATE TABLE test1 (id serial PRIMARY KEY, name VARCHAR(50));')
    @helper.src_sql('INSERT INTO test1 VALUES (1, \'a\'), (200, \'b\');')
    @helper.target_sql('CREATE TABLE test1 (id serial PRIMARY KEY, name VARCHAR(50));')
    @helper.target_sql('INSERT INTO test1 VALUES (1, \'a\'), (150, \'z\'), (200, \'c\');')

    refute @helper.run_diff(OPTIONS)

    assert_equal [
      'SELECT min(id) as k FROM test1',
      'SELECT max(id) as k FROM test1',
      'select id, name from test1 WHERE id >= 1 AND id < 1001 ORDER BY id',
      'select id, name from test1 WHERE id in (150, 200) ORDER BY id'
    ], sql_commands.uniq
  end
end
