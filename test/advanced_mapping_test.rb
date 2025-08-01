# frozen_string_literal: true

require 'minitest/autorun'
require_relative 'helper'
require 'json'

class AdvancedMappingTest < Minitest::Test
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

  def test_mapping_plus_one
    @helper.src_sql('CREATE TABLE test1 (id serial PRIMARY KEY, value INT);')
    @helper.src_sql('INSERT INTO test1 VALUES (1, 12), (2, 13);')
    @helper.target_sql('CREATE TABLE test1 (id serial PRIMARY KEY, value INT);')
    @helper.target_sql('INSERT INTO test1 VALUES (1, 13), (2, 14);')

    refute @helper.run_diff(OPTIONS)

    config = {
      'src' => {
        'value' => 'value + 1'
      }
    }

    assert @helper.run_diff(OPTIONS + " --advanced_mapping '#{JSON.dump(config)}'")

    assert_equal sql_commands, [
      'select id, value from test1 WHERE 1 = 1 ORDER BY id',
      'select id, value from test1 WHERE 1 = 1 ORDER BY id',
      'select id, value + 1 from test1 WHERE 1 = 1 ORDER BY id',
      'select id, value from test1 WHERE 1 = 1 ORDER BY id'
    ].freeze
  end

  def test_coalesce
    @helper.src_sql('CREATE TABLE test1 (id serial PRIMARY KEY, value INT);')
    @helper.src_sql('INSERT INTO test1 VALUES (1, 12), (2, 13);')
    @helper.target_sql('CREATE TABLE test1 (id serial PRIMARY KEY, value INT);')
    @helper.target_sql('INSERT INTO test1 VALUES (1, 12), (2, null);')

    refute @helper.run_diff(OPTIONS)

    config = {
      'target' => {
        'value' => 'coalesce(value, 13)'
      }
    }

    assert @helper.run_diff(OPTIONS + " --advanced_mapping '#{JSON.dump(config)}'")

    assert_equal sql_commands, [
      'select id, value from test1 WHERE 1 = 1 ORDER BY id',
      'select id, value from test1 WHERE 1 = 1 ORDER BY id',
      'select id, value from test1 WHERE 1 = 1 ORDER BY id',
      'select id, coalesce(value, 13) from test1 WHERE 1 = 1 ORDER BY id'
    ].freeze
  end
end
