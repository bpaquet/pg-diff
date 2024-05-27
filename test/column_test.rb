# frozen_string_literal: true

require 'minitest/autorun'

require_relative 'helper'
class ColumnTest < Minitest::Test
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

  def test_different_order
    @helper.src_sql('CREATE TABLE test1 (id serial PRIMARY KEY, name VARCHAR(50));')
    @helper.src_sql('INSERT INTO test1 VALUES (1, \'a\'), (2, \'b\');')
    @helper.target_sql('CREATE TABLE test1 (name VARCHAR(50), id serial PRIMARY KEY);')
    @helper.target_sql('INSERT INTO test1 VALUES (\'a\', 1), (\'b\', 2);')

    assert @helper.run_diff(OPTIONS)

    assert_equal sql_commands, EXPECTED_SQL
  end

  def test_different_colums
    @helper.src_sql('CREATE TABLE test1 (id serial PRIMARY KEY, name VARCHAR(50));')
    @helper.src_sql('INSERT INTO test1 VALUES (1, \'a\'), (2, \'b\');')
    @helper.target_sql('CREATE TABLE test1 (name VARCHAR(50), id serial PRIMARY KEY, foo text);')
    @helper.target_sql('INSERT INTO test1 VALUES (\'a\', 1, \'x\'), (\'b\', 2, \'z\');')

    assert @helper.run_diff(OPTIONS)

    assert_equal sql_commands, EXPECTED_SQL

    @helper.target_sql('UPDATE test1 SET name = \'y\' WHERE id = 1;')

    refute @helper.run_diff(OPTIONS)
  end

  def test_columns_filtering # rubocop:disable Minitest/MultipleAssertions
    @helper.src_sql('CREATE TABLE test1 (id serial PRIMARY KEY, name VARCHAR(50), x text);')
    @helper.src_sql('INSERT INTO test1 VALUES (1, \'a\', \'a\'), (2, \'b\', \'a\');')
    @helper.target_sql('CREATE TABLE test1 (id serial PRIMARY KEY, name VARCHAR(50), x text);')
    @helper.target_sql('INSERT INTO test1 VALUES (1, \'a\', \'a\'), (2, \'c\', \'a\');')

    refute @helper.run_diff(OPTIONS)

    assert @helper.run_diff("#{OPTIONS} --columns id,x")

    @helper.target_sql('UPDATE test1 SET x = \'y\' WHERE id = 1;')

    refute @helper.run_diff("#{OPTIONS} --columns id,x")

    assert @helper.run_diff("#{OPTIONS} --columns id")
  end
end
