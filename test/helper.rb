# frozen_string_literal: true

require 'pg'

class Helper
  def initialize
    @src = PG::Connection.open(dbname: 'test1', host: '127.0.0.1')
    @target = PG::Connection.open(dbname: 'test2', host: '127.0.0.1')
  end

  def src_sql(sql, params = [])
    @src.exec_params(sql, params)
  end

  def target_sql(sql, params = [])
    @target.exec_params(sql, params)
  end

  def run_diff(options, display_output: false)
    result = system("ruby lib/pg_diff.rb --src='postgresql://127.0.0.1/test1' " \
                    "--target='postgresql://127.0.0.1/test2' #{options} > /tmp/output 2>&1")
    puts File.read('/tmp/output') if display_output
    result
  end
end
