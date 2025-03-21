# frozen_string_literal: true

require 'minitest/autorun'

require_relative 'helper'
require_relative '../lib/extract_result_helper'

class ExtractResultToFileTest < Minitest::Test
  LOG_FILE = '/tmp/diff.log'
  OPTIONS = "--tables test1 --strategy=one_shot --extract_result_to_file #{LOG_FILE}".freeze

  def setup
    FileUtils.rm_f(LOG_FILE)
    @helper = Helper.new
    @helper.src_sql('DROP TABLE IF EXISTS test1;')
    @helper.target_sql('DROP TABLE IF EXISTS test1;')
  end

  def test_empty
    @helper.src_sql('CREATE TABLE test1 (id serial PRIMARY KEY, name VARCHAR(50));')
    @helper.target_sql('CREATE TABLE test1 (id serial PRIMARY KEY, name VARCHAR(50));')

    assert @helper.run_diff(OPTIONS)

    refute_path_exists LOG_FILE
  end

  def test_with_two_lines
    @helper.src_sql('CREATE TABLE test1 (id serial PRIMARY KEY, name VARCHAR(50));')
    @helper.src_sql('INSERT INTO test1 VALUES (1, \'a\'), (2, \'b\');')
    @helper.target_sql('CREATE TABLE test1 (id serial PRIMARY KEY, name VARCHAR(50));')
    @helper.target_sql('INSERT INTO test1 VALUES (1, \'a\'), (2, \'b\');')

    assert @helper.run_diff(OPTIONS)

    refute_path_exists LOG_FILE
  end

  def test_with_two_lines_and_diff # rubocop:disable Minitest/MultipleAssertions
    @helper.src_sql('CREATE TABLE test1 (id serial PRIMARY KEY, name VARCHAR(50));')
    @helper.src_sql('INSERT INTO test1 VALUES (1, \'a\'), (2, \'b\');')
    @helper.target_sql('CREATE TABLE test1 (id serial PRIMARY KEY, name VARCHAR(50));')

    refute @helper.run_diff(OPTIONS)

    assert_equal ['only_in_source: 1', 'only_in_source: 2'], File.readlines(LOG_FILE).map(&:strip)

    @helper.target_sql('INSERT INTO test1 VALUES (1, \'a\');')

    refute @helper.run_diff(OPTIONS)

    assert_equal ['only_in_source: 2'], File.readlines(LOG_FILE).map(&:strip)

    @helper.target_sql('INSERT INTO test1 VALUES (2, \'b\');')
    @helper.target_sql('INSERT INTO test1 VALUES (3, \'c\');')

    refute @helper.run_diff(OPTIONS)

    assert_equal ['only_in_target: 3'], File.readlines(LOG_FILE).map(&:strip)

    @helper.src_sql('INSERT INTO test1 VALUES (3, \'c\');')

    assert @helper.run_diff(OPTIONS)
    refute_path_exists LOG_FILE

    @helper.src_sql('UPDATE test1 set name = \'d\' where id = 3;')
    @helper.target_sql('INSERT INTO test1 VALUES (4, \'c\');')

    refute @helper.run_diff(OPTIONS)

    assert_equal ['changed: 3', 'only_in_target: 4'], File.readlines(LOG_FILE).map(&:strip)
  end

  def test_with_two_consecutives_diff
    @helper.src_sql('CREATE TABLE test1 (id serial PRIMARY KEY, name VARCHAR(50));')
    @helper.src_sql('INSERT INTO test1 VALUES (1, \'a\'), (2, \'b\');')
    @helper.target_sql('CREATE TABLE test1 (id serial PRIMARY KEY, name VARCHAR(50));')
    @helper.target_sql('INSERT INTO test1 VALUES (1, \'b\'), (2, \'c\');')

    refute @helper.run_diff(OPTIONS)
    assert_equal ['changed: 1', 'changed: 2'], File.readlines(LOG_FILE).map(&:strip)
  end
end
