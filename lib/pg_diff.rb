# frozen_string_literal: true

require 'optparse'
require 'logger'
require 'parallel'

require_relative 'stats'
require_relative 'extract_result_helper'
require_relative 'strategy/one_shot'
require_relative 'strategy/by_id'
require_relative 'strategy/by_timestamp'

options = {
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

  opts.on('--batch_size size', 'With by_id strategy, number of lines in each batch. ' \
                               'With by_timestamp strategy, number of days in each batch.') do |v|
    options[:batch_size] = v
  end

  opts.on('--log_level log_level', 'Log level') do |v|
    options[:log_level] = Logger.const_get(v.upcase)
  end
  opts.on('--columns columns_list', 'Columns list (comma separated) to use for comparison. ' \
                                    'If not specified, all columns will be used') do |v|
    options[:columns] = v.split(',')
  end

  opts.on('--extract_result_to_file file', 'Extract the result to a file') do |v|
    options[:extract_result_to_file] = v
  end
end.parse!

%i[src target tables].each do |key|
  raise "Missing --#{key} option" unless options[key]
end

require_relative 'psql'

logger = Logger.new($stdout)
logger.level = options[:log_level]

ExtractResultHelper.cleanup(options[:extract_result_to_file])

psql = Psql.new(options)
to_do = []
start = Time.now.to_f
options[:tables].split(',').each do |table|
  target_table = options[:table_mapping].gsub('<TABLE>', table)
  logger.warn("[#{table}] Preparing table")
  src_columns = psql.columns(table, options[:src])
  src_columns.select! { |k, _v| options[:columns].include?(k) } if options[:columns]
  key = options[:key]
  raise("[#{table}] Missing key #{key}") unless src_columns[key]
  raise("[#{table}] Key #{key} is nullable") unless src_columns[key] == 'NO'

  src_columns = src_columns.keys
  target_columns = psql.columns(target_table, options[:target]).keys

  if src_columns & target_columns != src_columns
    raise("[#{table}] Missing columns in target table #{target_table}: #{src_columns - target_columns}")
  end

  strategy_klass = "Strategy::#{options[:strategy].split('_').map(&:capitalize).join}"
  batches = Object.const_get(strategy_klass).new(options, psql, table, target_table).batches
  logger.info("[#{table}] Comparing with #{batches.size} batches, strategy: #{options[:strategy]}")
  logger.info("[#{table}] Key: #{key}, columns: #{src_columns.join(', ')}")
  if src_columns != target_columns
    logger.warn("[#{table}] Different columns in target table #{target_table}: #{target_columns - src_columns}")
  end
  to_do += batches.map { |batch| [table, src_columns, target_table, batch] }
end

logger.warn("Number of batches: #{to_do.size}, parallelism: #{options[:parallel]}")
Parallel.each( # rubocop:disable Metrics/BlockLength
  to_do,
  in_threads: options[:parallel],
  progress: $stdout.tty? ? 'Diffing ...' : nil
) do |table, columns, target_table, batch|
  src_sql = psql.build_copy(
    "select #{columns.join(', ')} from #{table} WHERE #{batch[:where]} ORDER BY #{options[:order_by]}"
  )
  target_sql = psql.build_copy(
    "select #{columns.join(', ')} from #{target_table} WHERE #{batch[:where]} ORDER BY #{options[:order_by]}"
  )
  target_sql.close

  diff_file = Tempfile.new("diff_#{table}")
  wc_file = Tempfile.new("wc_#{table}")
  wc_file.close
  command = 'diff --speed-large-file ' \
            "<(psql #{options[:src]} -v ON_ERROR_STOP=on -f #{src_sql.path} | tee >(wc -l > #{wc_file.path})) " \
            "<(psql #{options[:target]} -v ON_ERROR_STOP=on -f #{target_sql.path}) " \

  bash_file = Tempfile.new("bash_#{table}")
  bash_file.write(command)
  bash_file.close

  result = system("cat #{bash_file.path} | bash > #{diff_file.path} 2>&1")
  count = File.read(wc_file.path).to_i
  Stats.add_lines(count)

  if result
    logger.info("[#{table}] No error on batch #{batch[:name]}, #{count} lines")
  else
    puts File.read(diff_file.path)
    logger.error("[#{table}] Error on batch #{batch[:name]}, #{count} lines")
    Stats.add_error("[#{table}] Errors on batch: #{batch[:name]}")
    ExtractResultHelper.process(diff_file.path, options[:extract_result_to_file])
  end
  [src_sql, target_sql, wc_file, bash_file, diff_file].each(&:unlink)
end

duration = Time.now.to_f - start

if options[:extract_result_to_file] && File.exist?(options[:extract_result_to_file])
  logger.warn("Extracted result to #{options[:extract_result_to_file]}")
end

if Stats.all_errors.any?
  logger.error("Errors found: #{Stats.all_errors.count}: #{Stats.all_errors.join(', ')}, " \
               "#{Stats.all_lines} lines compared, duration: #{duration} seconds")
  exit 1
else
  logger.warn(
    "No error found in #{to_do.size} batches, #{Stats.all_lines} lines compared, duration: #{duration} seconds"
  )
end
