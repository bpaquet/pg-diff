# frozen_string_literal: true

require 'minitest/autorun'

require_relative 'helper'
class OneShotTest < Minitest::Test
  LOG_FILE = '/tmp/sql.log'
  OPTIONS = "--tables test1 --strategy=one_shot --record_sql_file #{LOG_FILE}".freeze

  def setup
    FileUtils.rm_f(LOG_FILE)
    @helper = Helper.new
    @helper.src_sql('DROP TABLE IF EXISTS test1;')
    @helper.target_sql('DROP TABLE IF EXISTS test1;')
  end

  def sql_commands
    File.readlines(LOG_FILE).map(&:strip).reject { |sql| sql.include?('information_schema.columns') }
  end

  EXPECTED_SQL = [
    'select id, name from test1 WHERE 1 = 1 ORDER BY id',
    'select id, name from test1 WHERE 1 = 1 ORDER BY id'
  ].freeze

  def test_empty
    @helper.src_sql('CREATE TABLE test1 (id serial PRIMARY KEY, name VARCHAR(50));')
    @helper.target_sql('CREATE TABLE test1 (id serial PRIMARY KEY, name VARCHAR(50));')

    assert @helper.run_diff(OPTIONS)

    assert_equal sql_commands, EXPECTED_SQL
  end

  def test_with_two_lines
    @helper.src_sql('CREATE TABLE test1 (id serial PRIMARY KEY, name VARCHAR(50));')
    @helper.src_sql('INSERT INTO test1 VALUES (1, \'a\'), (2, \'b\');')
    @helper.target_sql('CREATE TABLE test1 (id serial PRIMARY KEY, name VARCHAR(50));')
    @helper.target_sql('INSERT INTO test1 VALUES (1, \'a\'), (2, \'b\');')

    assert @helper.run_diff(OPTIONS)

    assert_equal sql_commands, EXPECTED_SQL
  end

  def test_with_two_lines_no_debug
    @helper.src_sql('CREATE TABLE test1 (id serial PRIMARY KEY, name VARCHAR(50));')
    @helper.src_sql('INSERT INTO test1 VALUES (1, \'a\'), (2, \'b\');')
    @helper.target_sql('CREATE TABLE test1 (id serial PRIMARY KEY, name VARCHAR(50));')
    @helper.target_sql('INSERT INTO test1 VALUES (1, \'a\'), (2, \'b\');')

    assert @helper.run_diff('--tables test1 --strategy=one_shot')
  end

  def test_with_two_lines_and_different_order
    @helper.src_sql('CREATE TABLE test1 (id serial PRIMARY KEY, name VARCHAR(50));')
    @helper.src_sql('INSERT INTO test1 VALUES (1, \'a\'), (2, \'b\');')
    @helper.target_sql('CREATE TABLE test1 (id serial PRIMARY KEY, name VARCHAR(50));')
    @helper.target_sql('INSERT INTO test1 VALUES (2, \'b\'), (1, \'a\');')

    assert @helper.run_diff(OPTIONS)
  end

  def test_with_two_lines_and_error
    @helper.src_sql('CREATE TABLE test1 (id serial PRIMARY KEY, name VARCHAR(50));')
    @helper.src_sql('INSERT INTO test1 VALUES (1, \'a\'), (2, \'b\');')
    @helper.target_sql('CREATE TABLE test1 (id serial PRIMARY KEY, name VARCHAR(50));')
    @helper.target_sql('INSERT INTO test1 VALUES (1, \'a\'), (2, \'c\');')

    refute @helper.run_diff(OPTIONS)
  end

  def test_with_one_two
    @helper.src_sql('CREATE TABLE test1 (id serial PRIMARY KEY, name VARCHAR(50));')
    @helper.src_sql('INSERT INTO test1 VALUES (1, \'a\');')
    @helper.target_sql('CREATE TABLE test1 (id serial PRIMARY KEY, name VARCHAR(50));')
    @helper.target_sql('INSERT INTO test1 VALUES (1, \'a\'), (2, \'b\');')

    refute @helper.run_diff(OPTIONS)
  end

  def test_with_two_one
    @helper.src_sql('CREATE TABLE test1 (id serial PRIMARY KEY, name VARCHAR(50));')
    @helper.src_sql('INSERT INTO test1 VALUES (1, \'a\'), (2, \'b\');')
    @helper.target_sql('CREATE TABLE test1 (id serial PRIMARY KEY, name VARCHAR(50));')
    @helper.target_sql('INSERT INTO test1 VALUES (1, \'a\');')

    refute @helper.run_diff(OPTIONS)
  end
end
