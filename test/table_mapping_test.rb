# frozen_string_literal: true

require 'minitest/autorun'

require_relative 'helper'
class TableMappingTest < Minitest::Test
  def setup
    @helper = Helper.new
    @helper.src_sql('DROP TABLE IF EXISTS test1;')
    @helper.target_sql('DROP TABLE IF EXISTS foo_test1;')
  end

  def test_with_two_lines
    @helper.src_sql('CREATE TABLE test1 (id serial PRIMARY KEY, name VARCHAR(50));')
    @helper.src_sql('INSERT INTO test1 VALUES (1, \'a\'), (200, \'x\');')
    @helper.target_sql('CREATE TABLE foo_test1 (id serial PRIMARY KEY, name VARCHAR(50));')
    @helper.target_sql('INSERT INTO foo_test1 VALUES (1, \'a\'), (200, \'x\');')

    refute @helper.run_diff('--tables test1')
    assert @helper.run_diff('--tables test1 --table_mapping foo_test1')
    assert @helper.run_diff("--tables test1 --table_mapping 'foo_<TABLE>'")
  end
end
