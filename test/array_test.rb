# frozen_string_literal: true

require 'minitest/autorun'
require_relative 'helper'
class ArrayTest < Minitest::Test
  def setup
    @helper = Helper.new
    @helper.src_sql('DROP TABLE IF EXISTS test1;')
    @helper.target_sql('DROP TABLE IF EXISTS test1;')
    @encoder_int = PG::TextEncoder::Array.new elements_type: PG::TextEncoder::Integer.new, needs_quotation: false
    @encoder_text = PG::TextEncoder::Array.new needs_quotation: true
  end

  def encode_int(array)
    @encoder_int.encode(array)
  end

  def encode_text(array)
    @encoder_text.encode(array)
  end

  def test_with_array_of_int
    @helper.src_sql('CREATE TABLE test1 (id serial PRIMARY KEY, val int[]);')
    @helper.src_sql('INSERT INTO test1 VALUES (1, $1), (2, $2);', ['{1, 2, 3}', '{}'])
    @helper.target_sql('CREATE TABLE test1 (id serial PRIMARY KEY, val int[]);')
    @helper.target_sql('INSERT INTO test1 VALUES (1, $1), (2, $2);', ['{1, 2, 3}', '{}'])

    assert @helper.run_diff('--tables test1')
    @helper.src_sql('UPDATE test1 SET val = $1 WHERE id = 1;', ['{1}'])

    refute @helper.run_diff('--tables test1')
  end

  def test_with_array_of_int_encoder
    @helper.src_sql('CREATE TABLE test1 (id serial PRIMARY KEY, val int[]);')
    @helper.src_sql('INSERT INTO test1 VALUES (1, $1), (2, $2);', [encode_int([1, 2, 3]), encode_int([])])
    @helper.target_sql('CREATE TABLE test1 (id serial PRIMARY KEY, val int[]);')
    @helper.target_sql('INSERT INTO test1 VALUES (1, $1), (2, $2);', [encode_int([1, 2, 3]), encode_int([])])

    assert @helper.run_diff('--tables test1')
    @helper.src_sql('UPDATE test1 SET val = $1 WHERE id = 1;', [encode_int([1])])

    refute @helper.run_diff('--tables test1')
  end

  def test_with_array_of_string
    @helper.src_sql('CREATE TABLE test1 (id serial PRIMARY KEY, val text[]);')
    @helper.src_sql('INSERT INTO test1 VALUES (1, $1), (2, $2);', [encode_text(%w[a b]), encode_text([])])
    @helper.target_sql('CREATE TABLE test1 (id serial PRIMARY KEY, val text[]);')
    @helper.target_sql('INSERT INTO test1 VALUES (1, $1), (2, $2);', [encode_text(%w[a b]), encode_text([])])

    assert @helper.run_diff('--tables test1')
    @helper.src_sql('UPDATE test1 SET val = $1 WHERE id = 1;', [encode_text(['1'])])

    refute @helper.run_diff('--tables test1')
  end

  def test_with_array_of_string_with_non_printable_char
    values = [encode_text([File.read(__FILE__), YAML.dump(a: 12)]), encode_text([])]
    @helper.src_sql('CREATE TABLE test1 (id serial PRIMARY KEY, val text[]);')
    @helper.src_sql('INSERT INTO test1 VALUES (1, $1), (2, $2);', values)
    @helper.target_sql('CREATE TABLE test1 (id serial PRIMARY KEY, val text[]);')
    @helper.target_sql('INSERT INTO test1 VALUES (1, $1), (2, $2);', values)

    assert @helper.run_diff('--tables test1')
    @helper.src_sql('UPDATE test1 SET val = $1 WHERE id = 1;', [encode_text([File.read('Gemfile')])])

    refute @helper.run_diff('--tables test1')
  end
end
