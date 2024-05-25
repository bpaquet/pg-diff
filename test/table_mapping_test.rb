# frozen_string_literal: true

require 'minitest/autorun'

require_relative 'helper'
class TableMappingTest < Minitest::Test
  def setup
    @helper = Helper.new
    @helper.src_sql('DROP TABLE IF EXISTS mapping_test1;')
    @helper.target_sql('DROP TABLE IF EXISTS foo_mapping_test1;')
  end

  def test_on_shot
    @helper.src_sql('CREATE TABLE mapping_test1 (id serial PRIMARY KEY, name VARCHAR(50));')
    @helper.src_sql('INSERT INTO mapping_test1 VALUES (1, \'a\'), (200, \'x\');')
    @helper.target_sql('CREATE TABLE foo_mapping_test1 (id serial PRIMARY KEY, name VARCHAR(50));')
    @helper.target_sql('INSERT INTO foo_mapping_test1 VALUES (1, \'a\'), (200, \'x\');')

    refute @helper.run_diff('--tables mapping_test1')
    assert @helper.run_diff('--tables mapping_test1 --table_mapping foo_mapping_test1')
    assert @helper.run_diff("--tables mapping_test1 --table_mapping 'foo_<TABLE>'")
  end

  def test_on_shot_by_id
    @helper.src_sql('CREATE TABLE mapping_test1 (id serial PRIMARY KEY, name VARCHAR(50));')
    @helper.src_sql('INSERT INTO mapping_test1 VALUES (1, \'a\'), (200, \'x\');')
    @helper.target_sql('CREATE TABLE foo_mapping_test1 (id serial PRIMARY KEY, name VARCHAR(50));')
    @helper.target_sql('INSERT INTO foo_mapping_test1 VALUES (1, \'a\'), (200, \'x\');')

    refute @helper.run_diff('--strategy=by_id --tables mapping_test1')
    assert @helper.run_diff('--strategy=by_id --tables mapping_test1 --table_mapping foo_mapping_test1')
    assert @helper.run_diff("--strategy=by_id --tables mapping_test1 --table_mapping 'foo_<TABLE>'")
  end
end
