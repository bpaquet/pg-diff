# frozen_string_literal: true

require 'pg'

class Helper
  def initialize # rubocop:disable Metrics/MethodLength
    @src = PG::Connection.open(
      dbname: 'test1',
      host: ENV['PG_HOST'] || 'localhost',
      user: ENV.fetch('PG_USER', nil),
      password: ENV.fetch('PG_PASSWORD', nil)
    )
    @target = PG::Connection.open(
      dbname: 'test2',
      host: ENV['PG_HOST'] || 'localhost',
      user: ENV.fetch('PG_USER', nil),
      password: ENV.fetch('PG_PASSWORD', nil)
    )
  end

  def src_sql(sql, params = [])
    @src.exec_params(sql, params)
  end

  def target_sql(sql, params = [])
    @target.exec_params(sql, params)
  end

  def run_diff(options, display_output: false)
    result = system("ruby lib/pg_diff.rb --src='postgresql://localhost/test1' " \
                    "--target='postgresql://localhost/test2' #{options} > /tmp/output 2>&1")
    puts File.read('/tmp/output') if display_output
    result
  end
end
