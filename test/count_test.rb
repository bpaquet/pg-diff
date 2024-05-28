# frozen_string_literal: true

require 'minitest/autorun'

require_relative 'helper'
class CountTest < Minitest::Test
  LOG_FILE = '/tmp/sql.log'
  OPTIONS = "--tables test1 --strategy=one_shot --record_sql_file #{LOG_FILE}".freeze

  def setup
    FileUtils.rm_f(LOG_FILE)
    @helper = Helper.new
    @helper.src_sql('DROP TABLE IF EXISTS test1;')
    @helper.target_sql('DROP TABLE IF EXISTS test1;')
  end

  def sql_commands
    File.readlines(LOG_FILE).map(&:strip).reject { |sql| sql.include?('information_schema.columns') }.uniq
  end

  def test_custom_select_count # rubocop:disable Minitest/MultipleAssertions
    @helper.src_sql('CREATE TABLE test1 (id serial PRIMARY KEY, name VARCHAR(50));')
    @helper.src_sql('INSERT INTO test1 VALUES (1, \'a\'), (2, \'b\');')
    @helper.target_sql('CREATE TABLE test1 (id serial PRIMARY KEY, name VARCHAR(50));')
    @helper.target_sql('INSERT INTO test1 VALUES (1, \'a\'), (2, \'c\');')

    refute @helper.run_diff(OPTIONS)

    assert_equal ['select id, name from test1 WHERE 1 = 1 ORDER BY id'], sql_commands

    FileUtils.rm_f(LOG_FILE)

    assert @helper.run_diff("#{OPTIONS} --custom_select='count(*)'")
    assert_equal ['select count(*) from test1 WHERE 1 = 1'], sql_commands
  end
end
