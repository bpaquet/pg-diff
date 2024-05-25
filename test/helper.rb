# frozen_string_literal: true

require 'pg'

class Helper
  def initialize
    @src = PG::Connection.open(dbname: 'test1', host: 'localhost')
    @target = PG::Connection.open(dbname: 'test2', host: 'localhost')
  end

  def src_sql(sql)
    @src.exec(sql)
  end

  def target_sql(sql)
    @target.exec(sql)
  end

  def run_diff(options)
    system "ruby lib/pg_diff.rb --src='postgresql://localhost/test1' --target='postgresql://localhost/test2' "\
    " #{options} > /dev/null 2>&1"
  end
end
