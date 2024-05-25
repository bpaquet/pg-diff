# frozen_string_literal: true

require 'optparse'
require 'logger'
require 'parallel'

require_relative 'strategy/one_shot'

options = {
  tmp_dir: '/tmp',
  psql: 'psql',
  log_level: 'info',
  order_by: 'id',
  strategy: 'one_shot',
  parallel: 4
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

  opts.on('--strategy strategy', 'Diff strategy: one_shot, by_id, by_date') do |v|
    options[:strategy] = v
  end

  opts.on('--parallel parallel', 'Number of parallel threads') do |v|
    options[:parallel] = v
  end
end.parse!

%i[src target tables].each do |key|
  raise "Missing --#{key} option" unless options[key]
end

require_relative 'psql'

logger = Logger.new($stdout)
logger.level = Logger.const_get(options[:log_level].upcase)

psql = Psql.new(options)
to_do = []
options[:tables].split(',').each do |table|
  strategy = case options[:strategy]
             when 'one_shot'
               Strategy::OneShot.new
             else
               raise "Unknown strategy: #{options[:strategy]}"
             end
  batches = strategy.batches
  logger.info("Comparing table #{table} with #{batches.size} batches, strategy: #{options[:strategy]}")
  to_do += batches.map { |batch| [table, batch] }
end

logger.info("Number of batch: #{to_do.size}")
Parallel.each(to_do, in_threads: options[:parallel]) do |table, batch|
  src_file = "#{options[:tmp_dir]}/pg_diff_src_#{table}_#{batch[:name]}"
  target_file = "#{options[:tmp_dir]}/pg_diff_target_#{table}_#{batch[:name]}"
  Parallel.each([[src_file, options[:src]], [target_file, options[:target]]], in_threads: 2) do |file, db|
    psql.run_psql_command(
      "\\copy ( select * from #{table} WHERE #{batch[:where]} ORDER BY #{options[:order_by]} ) to #{file}", db
    )
  end
  system("diff -du #{src_file} #{target_file}") || raise("Tables #{table} are different!")
  logger.info("Tables #{table} are the same, file size: #{File.size(src_file)}")
  File.unlink(src_file)
  File.unlink(target_file)
end
