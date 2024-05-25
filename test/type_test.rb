# frozen_string_literal: true

require 'minitest/autorun'
require 'json'
require 'yaml'
require_relative 'helper'
class TypeTest < Minitest::Test
  def setup
    @helper = Helper.new
    @helper.src_sql('DROP TABLE IF EXISTS test1;')
    @helper.target_sql('DROP TABLE IF EXISTS test1;')
  end

  def test_with_varchar
    @helper.src_sql('CREATE TABLE test1 (id serial PRIMARY KEY, name VARCHAR(50));')
    @helper.src_sql('INSERT INTO test1 VALUES (1, \'a\'), (2, \'b\');')
    @helper.target_sql('CREATE TABLE test1 (id serial PRIMARY KEY, name VARCHAR(50));')
    @helper.target_sql('INSERT INTO test1 VALUES (1, \'a\'), (2, \'b\');')

    assert @helper.run_diff('--tables test1')
    @helper.src_sql('UPDATE test1 SET name = \'b\' WHERE id = 1;')

    refute @helper.run_diff('--tables test1')
  end

  def test_with_json
    @helper.src_sql('CREATE TABLE test1 (id serial PRIMARY KEY, name jsonb);')
    @helper.src_sql('INSERT INTO test1 VALUES (1, \'{"a":2}\'), (2, \'{"x":"y"}\');')
    @helper.target_sql('CREATE TABLE test1 (id serial PRIMARY KEY, name jsonb);')
    @helper.target_sql('INSERT INTO test1 VALUES (1, \'{"a":2}\'), (2, \'{"x":"y"}\');')

    assert @helper.run_diff('--tables test1')
    @helper.src_sql('UPDATE test1 SET name = \'{"a":3}\' WHERE id = 1;')

    refute @helper.run_diff('--tables test1')
  end

  def test_with_varchar_vs_text
    @helper.src_sql('CREATE TABLE test1 (id serial PRIMARY KEY, name VARCHAR(50));')
    @helper.src_sql('INSERT INTO test1 VALUES (1, \'a\'), (2, \'b\');')
    @helper.target_sql('CREATE TABLE test1 (id serial PRIMARY KEY, name text);')
    @helper.target_sql('INSERT INTO test1 VALUES (1, \'a\'), (2, \'b\');')

    assert @helper.run_diff('--tables test1')
    @helper.src_sql('UPDATE test1 SET name = \'xxxx\' WHERE id = 1;')

    refute @helper.run_diff('--tables test1')
  end

  def test_with_varchar_with_non_printable
    @helper.src_sql('CREATE TABLE test1 (id serial PRIMARY KEY, name text);')
    @helper.src_sql('INSERT INTO test1 VALUES (1, $1), (2, $2);',
                    [File.read(__FILE__), JSON.dump({ a: "hello\n\rfoobar" })])
    @helper.target_sql('CREATE TABLE test1 (id serial PRIMARY KEY, name text);')
    @helper.target_sql('INSERT INTO test1 VALUES (1, $1), (2, $2);',
                       [File.read(__FILE__), JSON.dump({ a: "hello\n\rfoobar" })])

    assert @helper.run_diff('--tables test1')
    @helper.src_sql('UPDATE test1 SET name = $1 WHERE id = 1;', [File.read('Gemfile')])

    refute @helper.run_diff('--tables test1')
  end

  def test_with_varchar_with_yaml
    @helper.src_sql('CREATE TABLE test1 (id serial PRIMARY KEY, name text);')
    @helper.src_sql('INSERT INTO test1 VALUES (1, $1), (2, $2);',
                    ['a', YAML.dump(a: 1, b: 'f')])
    @helper.target_sql('CREATE TABLE test1 (id serial PRIMARY KEY, name text);')
    @helper.target_sql('INSERT INTO test1 VALUES (1, $1), (2, $2);',
                       ['a', YAML.dump(a: 1, b: 'f')])

    assert @helper.run_diff('--tables test1')
    @helper.src_sql('UPDATE test1 SET name = $1 WHERE id = 1;', [File.read('Gemfile')])

    refute @helper.run_diff('--tables test1')
  end

  def test_with_bool
    @helper.src_sql('CREATE TABLE test1 (id serial PRIMARY KEY, val bool);')
    @helper.src_sql('INSERT INTO test1 VALUES (1, false), (2, true);')
    @helper.target_sql('CREATE TABLE test1 (id serial PRIMARY KEY, val bool);')
    @helper.target_sql('INSERT INTO test1 VALUES (1, false), (2, true);')

    assert @helper.run_diff('--tables test1')
    @helper.src_sql('UPDATE test1 SET val = true WHERE id = 1;')

    refute @helper.run_diff('--tables test1')
  end

  def test_with_timestamp
    now = Time.now
    @helper.src_sql('CREATE TABLE test1 (id serial PRIMARY KEY, val timestamp);')
    @helper.src_sql('INSERT INTO test1 VALUES (1, $1), (2, $2);', [now, now + 10])
    @helper.target_sql('CREATE TABLE test1 (id serial PRIMARY KEY, val timestamp);')
    @helper.target_sql('INSERT INTO test1 VALUES (1, $1), (2, $2);', [now, now + 10])

    assert @helper.run_diff('--tables test1')
    @helper.src_sql('UPDATE test1 SET val = $1 WHERE id = 1;', [now - 1])

    refute @helper.run_diff('--tables test1')
  end
end
