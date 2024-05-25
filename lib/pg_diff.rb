# frozen_string_literal: true

require 'optparse'
require 'logger'
require 'parallel'

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

  opts.on('--target target_url', 'Url of the target database, like postgresql://127.0.0.1/to') do |v|
    options[:target] = v
  end

  opts.on('--tables tables', 'Comma separated list of table to compare') do |v|
    options[:tables] = v
  end

  opts.on('--order_by order_by', 'Order by column') do |v|
    options[:order_by] = v
  end
end.parse!

%i[src target tables].each do |key|
  raise "Missing --#{key} option" unless options[key]
end

require_relative 'psql'

logger = Logger.new($stdout)
logger.level = Logger.const_get(options[:log_level].upcase)

psql = Psql.new(options)
options[:tables].split(',').each do |table|
  src_file = "#{options[:tmp_dir]}/pg_diff_src_#{table}"
  target_file = "#{options[:tmp_dir]}/pg_diff_target_#{table}"
  Parallel.each([[src_file, options[:src]], [target_file, options[:target]]], in_threads: 2) do |file, db|
    psql.run_psql_command("\\copy ( select * from #{table} ORDER BY #{options[:order_by]} ) to #{file}", db)
  end
  system("diff -du #{src_file} #{target_file}") || raise("Tables #{table} are different!")
  logger.info("Tables #{table} are the same, file size: #{File.size(src_file)}")
  File.unlink(src_file)
  File.unlink(target_file)
end
