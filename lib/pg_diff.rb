# frozen_string_literal: true

require 'optparse'
require 'logger'
require 'parallel'

require_relative 'stats'
require_relative 'strategy/one_shot'
require_relative 'strategy/by_id'
require_relative 'strategy/by_timestamp'

options = {
  tmp_dir: '/tmp',
  psql: 'psql',
  log_level: Logger::INFO,
  order_by: 'id',
  strategy: 'one_shot',
  parallel: 4,
  batch_size: 1000,
  key: 'id',
  table_mapping: '<TABLE>'
}

OptionParser.new do |opts| # rubocop:disable Metrics/BlockLength
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

  opts.on('--table_mapping mapping', 'Target table name. Can contain <TABLE>, which is the orignal table name') do |v|
    options[:table_mapping] = v
  end

  opts.on('--order_by order_by', 'Order by column') do |v|
    options[:order_by] = v
  end

  opts.on('--strategy strategy', 'Diff strategy: one_shot, by_id, by_date') do |v|
    options[:strategy] = v
  end

  opts.on('--parallel parallel', 'Number of parallel threads') do |v|
    options[:parallel] = v.to_i
  end

  opts.on('--record_sql_file file', 'File to log all sql request') do |v|
    options[:record_sql_file] = v
  end

  opts.on('--key key', 'Column used to split') do |v|
    options[:key] = v
  end

  opts.on('--key_start key', 'Where to start the diff. If not specified, min(key) will be called') do |v|
    options[:key_start] = v
  end

  opts.on('--key_stop key', 'Where to stop the diff. If not specified, max(key) will be called') do |v|
    options[:key_stop] = v
  end

  opts.on('--batch_size size', 'Number of lines in each batch') do |v|
    options[:batch_size] = v.to_f
  end

  opts.on('--log_level log_level', 'Log level') do |v|
    options[:log_level] = Logger.const_get(v.upcase)
  end
end.parse!

%i[src target tables].each do |key|
  raise "Missing --#{key} option" unless options[key]
end

require_relative 'psql'

logger = Logger.new($stdout)
logger.level = options[:log_level]

psql = Psql.new(options)
to_do = []
options[:tables].split(',').each do |table|
  target_table = options[:table_mapping].gsub('<TABLE>', table)
  logger.info("Preparing table #{table}")
  strategy_klass = "Strategy::#{options[:strategy].split('_').map(&:capitalize).join}"
  batches = Object.const_get(strategy_klass).new(options, psql, table, target_table).batches
  logger.info("Comparing table #{table} with #{batches.size} batches, strategy: #{options[:strategy]}")
  to_do += batches.map { |batch| [table, target_table, batch] }
end

logger.warn("Number of batches: #{to_do.size}")
Parallel.each(to_do, in_threads: options[:parallel], progress: 'Diffing ...') do |table, target_table, batch|
  src_file = "#{options[:tmp_dir]}/pg_diff_src_#{table}_#{batch[:name]}"
  target_file = "#{options[:tmp_dir]}/pg_diff_target_#{target_table}_#{batch[:name]}"
  Parallel.each(
    [
      [src_file, table, options[:src]],
      [target_file, target_table, options[:target]]
    ], in_threads: 2
  ) do |file, real_table, db|
    psql.run_copy("select * from #{real_table} WHERE #{batch[:where]} ORDER BY #{options[:order_by]}", file, db)
  end
  result = system("diff -du #{src_file} #{target_file}")
  count = `wc -l #{src_file}`.to_i
  Stats.add_lines(count)
  size = File.size(src_file)
  File.unlink(src_file)
  File.unlink(target_file)
  if result
    logger.info("No error on batch #{batch[:name]}, file size: #{size}, #{count} lines")
  else
    logger.error("Error on batch #{batch[:name]} file size: #{size}, #{count} lines")
    Stats.add_error("Errors on batch: #{batch[:name]}")
  end
end

if Stats.all_errors.any?
  logger.error("Errors found: #{Stats.all_errors.count}: #{Stats.all_errors.join(', ')}")
  exit 1
else
  logger.warn("No error found in #{to_do.size} batches, #{Stats.all_lines} lines compared")
end
