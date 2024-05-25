# frozen_string_literal: true

require 'minitest/autorun'
require_relative '../helper'
class NumericTypeTest < Minitest::Test
  def setup
    @helper = Helper.new
    @helper.src_sql('DROP TABLE IF EXISTS test1;')
    @helper.target_sql('DROP TABLE IF EXISTS test1;')
  end

  def test_with_float
    @helper.src_sql('CREATE TABLE test1 (id serial PRIMARY KEY, val float);')
    @helper.src_sql('INSERT INTO test1 VALUES (1, 1.1), (2, 2.4);')
    @helper.target_sql('CREATE TABLE test1 (id serial PRIMARY KEY, val float);')
    @helper.target_sql('INSERT INTO test1 VALUES (1, 1.1), (2, 2.4);')

    assert @helper.run_diff('--tables test1')
    @helper.src_sql('UPDATE test1 SET val = 2.2/3 WHERE id = 1;')

    refute @helper.run_diff('--tables test1')
    @helper.target_sql('UPDATE test1 SET val = 2.2/3 WHERE id = 1;')

    assert @helper.run_diff('--tables test1')
  end

  def test_with_float_vs_decimal_without_precision_issue
    @helper.src_sql('CREATE TABLE test1 (id serial PRIMARY KEY, val float);')
    @helper.src_sql('INSERT INTO test1 VALUES (1, 1.1), (2, 2.4);')
    @helper.target_sql('CREATE TABLE test1 (id serial PRIMARY KEY, val decimal);')
    @helper.target_sql('INSERT INTO test1 VALUES (1, 1.1), (2, 2.4);')

    assert @helper.run_diff('--tables test1')
    @helper.src_sql('UPDATE test1 SET val = 2.2/3 WHERE id = 1;')

    refute @helper.run_diff('--tables test1')
    @helper.target_sql('UPDATE test1 SET val = 2.2/3 WHERE id = 1;')

    refute @helper.run_diff('--tables test1') # false because of precision issue
  end

  def test_with_int_vs_bigint
    @helper.src_sql('CREATE TABLE test1 (id serial PRIMARY KEY, val int);')
    @helper.src_sql('INSERT INTO test1 VALUES (1, 12), (2, 42);')
    @helper.target_sql('CREATE TABLE test1 (id serial PRIMARY KEY, val bigint);')
    @helper.target_sql('INSERT INTO test1 VALUES (1, 12), (2, 42);')

    assert @helper.run_diff('--tables test1')
    @helper.src_sql('UPDATE test1 SET val = 13 WHERE id = 1;')

    refute @helper.run_diff('--tables test1')
  end
end
