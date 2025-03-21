# frozen_string_literal: true

require 'minitest/autorun'

require_relative 'helper'
class WhereClauseTest < Minitest::Test
  def setup
    @helper = Helper.new
    @helper.src_sql('DROP TABLE IF EXISTS test1;')
    @helper.target_sql('DROP TABLE IF EXISTS test1;')
  end

  def test_where # rubocop:disable Minitest/MultipleAssertions
    @helper.src_sql('CREATE TABLE test1 (id serial PRIMARY KEY, name VARCHAR(50));')
    @helper.src_sql('INSERT INTO test1 VALUES (1, \'a\'), (200, \'x\');')
    @helper.target_sql('CREATE TABLE test1 (id serial PRIMARY KEY, name VARCHAR(50));')
    @helper.target_sql('INSERT INTO test1 VALUES (1, \'a\');')

    refute @helper.run_diff('--tables test1')
    assert @helper.run_diff('--tables test1 --where_from "id != 200"')

    @helper.target_sql('INSERT INTO test1 VALUES (300, \'x\');')

    refute @helper.run_diff('--tables test1 --where_from "id != 200"')
    assert @helper.run_diff('--tables test1 --where_from "id != 200" --where_target "id != 300"')
  end
end
