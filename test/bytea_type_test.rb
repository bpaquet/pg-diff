# frozen_string_literal: true

require 'minitest/autorun'
require_relative 'helper'
class ByteaTypeTest < Minitest::Test
  def setup
    @helper = Helper.new
    @helper.src_sql('DROP TABLE IF EXISTS test1;')
    @helper.target_sql('DROP TABLE IF EXISTS test1;')
    @encoder = PG::TextEncoder::Bytea.new
  end

  def encode(binary)
    @encoder.encode(binary)
  end

  def test_with_bytea
    @helper.src_sql('CREATE TABLE test1 (id serial PRIMARY KEY, val bytea);')
    @helper.src_sql('INSERT INTO test1 VALUES (1, \'a\'), (2, \'b\');')
    @helper.target_sql('CREATE TABLE test1 (id serial PRIMARY KEY, val bytea);')
    @helper.target_sql('INSERT INTO test1 VALUES (1, \'a\'), (2, \'b\');')

    assert @helper.run_diff('--tables test1')
    @helper.src_sql('UPDATE test1 SET val = \'b\' WHERE id = 1;')

    refute @helper.run_diff('--tables test1')
  end

  def test_with_bytea_binary
    binary = [encode(100.times.map { rand(256).chr }.join)]
    @helper.src_sql('CREATE TABLE test1 (id serial PRIMARY KEY, val bytea);')
    @helper.src_sql('INSERT INTO test1 VALUES (1, $1);', binary)
    @helper.target_sql('CREATE TABLE test1 (id serial PRIMARY KEY, val bytea);')
    @helper.target_sql('INSERT INTO test1 VALUES (1, $1);', binary)

    assert @helper.run_diff('--tables test1')
    @helper.src_sql('UPDATE test1 SET val = $1 WHERE id = 1;', ['a'])

    refute @helper.run_diff('--tables test1')
  end

  def test_with_large_bytea_binary
    binary =
      File.open('/dev/random', 'rb') do |f|
        [encode(f.read(1024 * 1024 * 10))]
      end
    @helper.src_sql('CREATE TABLE test1 (id serial PRIMARY KEY, val bytea);')
    @helper.src_sql('INSERT INTO test1 VALUES (1, $1);', binary)
    @helper.target_sql('CREATE TABLE test1 (id serial PRIMARY KEY, val bytea);')
    @helper.target_sql('INSERT INTO test1 VALUES (1, $1);', binary)

    assert @helper.run_diff('--tables test1')
  end
end
