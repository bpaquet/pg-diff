# frozen_string_literal: true

require 'optparse'
require 'logger'
require 'parallel'

require_relative 'stats'
require_relative 'table_comparer'
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

  opts.on('--psql psql_binary', 'Path to the psql binary. Default: psql.') do |v|
    options[:psql] = v
  end

  opts.on('--log_level log_level', 'Log level. Default: info.') do |v|
    options[:log_level] = Logger.const_get(v.upcase)
  end

  opts.on('--src source_url', 'Url of the source database, like postgresql://127.0.0.1/src.') do |v|
    options[:src] = v
  end

  opts.on('--target target_url', 'Url of the target database, like postgresql://127.0.0.1/to.') do |v|
    options[:target] = v
  end

  opts.on('--tables tables', 'Comma separated list of table to diff.') do |v|
    options[:tables] = v
  end

  opts.on('--table_mapping mapping', 'Target table name. Can contain <TABLE>, which is the orignal table name. ' \
                                     'Example: new_namespace.<TABLE>. Default: <TABLE>.') do |v|
    options[:table_mapping] = v
  end

  opts.on('--order_by order_by', 'Order by clause to be used with select sql queries. Default: id.') do |v|
    options[:order_by] = v
  end

  opts.on('--strategy strategy', 'Diff strategy: one_shot, by_id, by_timestamp. Default: one_shot.') do |v|
    options[:strategy] = v
  end

  opts.on('--parallel parallel', 'Level of parallelism. Default: 4.') do |v|
    options[:parallel] = v.to_i
  end

  opts.on('--key key',
          'Column used to split the work to do. Default is id, but can be replaced by created_at ' \
          'with by_timestamp strategy.') do |v|
    options[:key] = v
  end

  opts.on('--key_start key', 'Where to start the diff. If not specified, min(key) will be used') do |v|
    options[:key_start] = v
  end

  opts.on('--key_stop key', 'Where to stop the diff. If not specified, max(key) + 1 will be used') do |v|
    options[:key_stop] = v
  end

  opts.on('--batch_size size', 'With by_id strategy, number of lines in each batch. ' \
                               'With by_timestamp strategy, number of days in each batch. Default: 10000.') do |v|
    options[:batch_size] = v
  end

  opts.on('--columns columns_list', 'Columns list (comma separated) to use for comparison. ' \
                                    'If not specified, all columns will be used') do |v|
    options[:columns] = v.split(',')
  end

  opts.on('--record_sql_file file', 'File to log all sql request. Mosty lused in tests') do |v|
    options[:record_sql_file] = v
  end

  opts.on('--extract_result_to_file file',
          'Extract the result to a file to reuse it after. Can contain <TABLE> which will be ' \
          'replaced by the current table name.') do |v|
    options[:extract_result_to_file] = v
  end

  opts.on('--limit_to_the_past_minutes minutes', 'Add a where condition like [key] < now - x minutes') do |v|
    options[:limit_to_the_past_minutes] = v.to_i
  end

  opts.on('--limit_to_the_past_key key', 'Key to use with limit_to_the_past_minutes. ' \
                                         'Default is the key from --key') do |v|
    options[:limit_to_the_past_key] = v
  end

  opts.on('--custom_select custom_select', 'Instead of doing a full diff, use a custom select. ' \
                                           'For example, count(*) can be used to compare append only table.') do |v|
    options[:custom_select] = v
  end
end.parse!

%i[src target tables].each do |key|
  raise "Missing --#{key} option" unless options[key]
end

require_relative 'psql'

logger = Logger.new($stdout)
logger.level = options[:log_level]

if options[:limit_to_the_past_minutes]
  where_key = options[:limit_to_the_past_key] || options[:key]
  options[:where_clause] = " AND #{where_key} < '#{Time.now - (options[:limit_to_the_past_minutes] * 60)}'"
  logger.info("Adding where clause: #{options[:where_clause][5..]}")
end

logger.info("Using custom select: #{options[:custom_select]}") if options[:custom_select]

psql = Psql.new(options)
to_do = []
start = Time.now.to_f
options[:tables].split(',').map do |table|
  handler = TableComparer.new(options, psql, table)
  handler.configure_columns!
  to_do += handler.compute_batches
end

logger.warn("Number of batches: #{to_do.size}, parallelism: #{options[:parallel]}, mode: #{options[:mode]}")
Parallel.each(
  to_do,
  in_threads: options[:parallel],
  progress: $stdout.tty? ? 'Diffing ...' : nil
) do |handler, batch|
  handler.process_batch(batch)
end

duration = Time.now.to_f - start

if Stats.all_errors.any?
  logger.error("Errors found: #{Stats.all_errors.count}: #{Stats.all_errors.join(', ')}, " \
               "#{Stats.all_lines} lines compared, duration: #{duration} seconds")
  exit 1
else
  logger.warn(
    "No error found in #{to_do.size} batches, #{Stats.all_lines} lines compared, duration: #{duration} seconds"
  )
end
