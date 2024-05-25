# frozen_string_literal: true

require 'optparse'
require 'logger'

options = {
  tmp_dir: '/tmp',
  psql: 'psql',
  log_level: 'info',
  order_by: 'id'
}

OptionParser.new do |opts|
  opts.banner = 'Usage: pg-diff [options]'

  opts.on('--tmp tmp_dir', 'Sepcify the tmp directory to use') do |v|
    options[:tmp_dir] = v
  end
  opts.on('--psql psql_binary', 'psql binary') do |v|
    options[:psql] = v
  end

  opts.on('--src source_url', 'Url of the source database, like postgresql://127.0.0.1/src') do |v|
    options[:src] = v
  end

  opts.on('--to to_url', 'Url of the target database, like postgresql://127.0.0.1/to') do |v|
    options[:to] = v
  end

  opts.on('--tables tables', 'Comma separated list of table to compare') do |v|
    options[:tables] = v
  end

  opts.on('--order_by order_by', 'Order by column') do |v|
    options[:order_by] = v
  end
end.parse!

%i[src to tables].each do |key|
  raise "Missing --#{key} option" unless options[key]
end

require_relative 'psql'

logger = Logger.new($stdout)
logger.level = Logger.const_get(options[:log_level].upcase)

psql = Psql.new(options)
options[:tables].split(',').each do |table|
  src_file = "#{options[:tmp_dir]}/pg_diff_src_#{table}"
  to_file = "#{options[:tmp_dir]}/pg_diff_to_#{table}"
  psql.run_psql_command("\\copy ( select * from #{table} ORDER BY #{options[:order_by]} ) to #{src_file}",
                        options[:src])
  psql.run_psql_command("\\copy ( select * from #{table} ORDER BY #{options[:order_by]} ) to #{to_file}", options[:to])
  system("diff -du #{src_file} #{to_file}") || raise("Tables #{table} are different!")
  logger.info("Tables #{table} are the same")
end
