# frozen_string_literal: true

require 'minitest/autorun'

require_relative 'helper'
class ByIdTest < Minitest::Test
  LOG_FILE = '/tmp/sql.log'
  OPTIONS = "--tables test1 --strategy=by_id --record_sql_file #{LOG_FILE}".freeze

  def setup
    FileUtils.rm_f(LOG_FILE)
    @helper = Helper.new
    @helper.src_sql('DROP TABLE IF EXISTS test1;')
    @helper.target_sql('DROP TABLE IF EXISTS test1;')
  end

  def sql_commands
    File.readlines(LOG_FILE).map(&:strip)
  end

  def test_empty
    @helper.src_sql('CREATE TABLE test1 (id serial PRIMARY KEY, name VARCHAR(50));')
    @helper.target_sql('CREATE TABLE test1 (id serial PRIMARY KEY, name VARCHAR(50));')

    assert @helper.run_diff(OPTIONS)

    assert_equal [
      'SELECT min(id) as k FROM test1',
      'SELECT max(id) as k FROM test1',
      'select * from test1 WHERE id >= 0 AND id < 1000 ORDER BY id'
    ], sql_commands.uniq
  end

  def test_with_two_lines
    @helper.src_sql('CREATE TABLE test1 (id serial PRIMARY KEY, name VARCHAR(50));')
    @helper.src_sql('INSERT INTO test1 VALUES (1, \'a\'), (200, \'b\');')
    @helper.target_sql('CREATE TABLE test1 (id serial PRIMARY KEY, name VARCHAR(50));')
    @helper.target_sql('INSERT INTO test1 VALUES (1, \'a\'), (200, \'b\');')

    assert @helper.run_diff(OPTIONS)

    assert_equal [
      'SELECT min(id) as k FROM test1',
      'SELECT max(id) as k FROM test1',
      'select * from test1 WHERE id >= 1 AND id < 1001 ORDER BY id'
    ], sql_commands.uniq
  end

  def test_with_two_lines_negative_start # rubocop:disable Metrics/MethodLength
    @helper.src_sql('CREATE TABLE test1 (id serial PRIMARY KEY, name VARCHAR(50));')
    @helper.src_sql('INSERT INTO test1 VALUES (-1000, \'a\'), (200, \'b\');')
    @helper.target_sql('CREATE TABLE test1 (id serial PRIMARY KEY, name VARCHAR(50));')
    @helper.target_sql('INSERT INTO test1 VALUES (-1000, \'a\'), (200, \'b\');')

    assert @helper.run_diff(OPTIONS)

    assert_equal [
      'SELECT min(id) as k FROM test1',
      'SELECT max(id) as k FROM test1',
      'select * from test1 WHERE id >= -1000 AND id < 0 ORDER BY id',
      'select * from test1 WHERE id >= 0 AND id < 1000 ORDER BY id'
    ], sql_commands.uniq
  end

  def test_with_two_lines_batch_size # rubocop:disable Metrics/MethodLength
    @helper.src_sql('CREATE TABLE test1 (id serial PRIMARY KEY, name VARCHAR(50));')
    @helper.src_sql('INSERT INTO test1 VALUES (1, \'a\'), (200, \'b\');')
    @helper.target_sql('CREATE TABLE test1 (id serial PRIMARY KEY, name VARCHAR(50));')
    @helper.target_sql('INSERT INTO test1 VALUES (1, \'a\'), (200, \'b\');')

    assert @helper.run_diff("#{OPTIONS} --batch_size=70")

    assert_equal [
      'SELECT min(id) as k FROM test1',
      'SELECT max(id) as k FROM test1',
      'select * from test1 WHERE id >= 1 AND id < 71 ORDER BY id',
      'select * from test1 WHERE id >= 71 AND id < 141 ORDER BY id',
      'select * from test1 WHERE id >= 141 AND id < 211 ORDER BY id'
    ], sql_commands.uniq
  end

  def test_with_two_lines_key_start_stop
    @helper.src_sql('CREATE TABLE test1 (id serial PRIMARY KEY, name VARCHAR(50));')
    @helper.src_sql('INSERT INTO test1 VALUES (1, \'a\'), (200, \'b\');')
    @helper.target_sql('CREATE TABLE test1 (id serial PRIMARY KEY, name VARCHAR(50));')
    @helper.target_sql('INSERT INTO test1 VALUES (1, \'a\'), (200, \'c\');')

    refute @helper.run_diff(OPTIONS)

    FileUtils.rm_f(LOG_FILE)

    assert @helper.run_diff("#{OPTIONS} --key_start=-50 --key_stop=20 --batch_size=100", display_output: false)

    assert_equal [
      'select * from test1 WHERE id >= -50 AND id < 50 ORDER BY id'
    ], sql_commands.uniq
  end

  def test_with_two_lines_max # rubocop:disable Metrics/MethodLength
    @helper.src_sql('CREATE TABLE test1 (id serial PRIMARY KEY, name VARCHAR(50));')
    @helper.src_sql('INSERT INTO test1 VALUES (1, \'a\'), (200, \'b\');')
    @helper.target_sql('CREATE TABLE test1 (id serial PRIMARY KEY, name VARCHAR(50));')
    @helper.target_sql('INSERT INTO test1 VALUES (1, \'a\'), (2000, \'b\');')

    refute @helper.run_diff(OPTIONS)

    assert_equal [
      'SELECT min(id) as k FROM test1',
      'SELECT max(id) as k FROM test1',
      'select * from test1 WHERE id >= 1 AND id < 1001 ORDER BY id',
      'select * from test1 WHERE id >= 1001 AND id < 2001 ORDER BY id'
    ], sql_commands.uniq
  end

  def test_with_two_lines_min
    @helper.src_sql('CREATE TABLE test1 (id serial PRIMARY KEY, name VARCHAR(50));')
    @helper.src_sql('INSERT INTO test1 VALUES (1, \'a\'), (200, \'b\');')
    @helper.target_sql('CREATE TABLE test1 (id serial PRIMARY KEY, name VARCHAR(50));')
    @helper.target_sql('INSERT INTO test1 VALUES (-200, \'a\'), (200, \'b\');')

    refute @helper.run_diff(OPTIONS)

    assert_equal [
      'SELECT min(id) as k FROM test1',
      'SELECT max(id) as k FROM test1',
      'select * from test1 WHERE id >= -200 AND id < 800 ORDER BY id'
    ], sql_commands.uniq
  end
end
