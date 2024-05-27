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

  opts.on('--batch_size size', 'With by_id strategy, number of lines in each batch. ' \
                               'With by_timestamp strategy, number of days in each batch.') do |v|
    options[:batch_size] = v
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
  logger.warn("[#{table}] Preparing table")
  src_columns = psql.columns(table, options[:src])
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
  logger.info("[#{table}] key: #{key}, columns: #{src_columns.join(', ')}")
  if src_columns != target_columns
    logger.warn("[#{table}] Different columns in target table #{target_table}: #{target_columns - src_columns}")
  end
  to_do += batches.map { |batch| [table, src_columns, target_table, batch] }
end

logger.warn("Number of batches: #{to_do.size}, parallelism: #{options[:parallel]}")
Parallel.each(to_do, in_threads: options[:parallel], progress: 'Diffing ...') do |table, columns, target_table, batch| # rubocop:disable Metrics/BlockLength
  src_sql = psql.build_copy(
    "select #{columns.join(', ')} from #{table} WHERE #{batch[:where]} ORDER BY #{options[:order_by]}"
  )
  target_sql = psql.build_copy(
    "select #{columns.join(', ')} from #{target_table} WHERE #{batch[:where]} ORDER BY #{options[:order_by]}"
  )
  target_sql.close

  wc_file = Tempfile.new("wc_#{table}")
  wc_file.close
  command = 'diff --speed-large-file ' \
            "<(psql #{options[:src]} -v ON_ERROR_STOP=on -f #{src_sql.path} | tee >(wc -l > #{wc_file.path})) " \
            "<(psql #{options[:target]} -v ON_ERROR_STOP=on -f #{target_sql.path}) " \

  bash = Tempfile.new("bash_#{table}")
  bash.write(command)
  bash.close

  result = system("cat #{bash.path} | bash")
  count = File.read(wc_file.path).to_i
  Stats.add_lines(count)
  [src_sql, target_sql, wc_file, bash]
  File.unlink(src_sql)
  File.unlink(target_sql)
  File.unlink(wc_file)
  File.unlink(bash)

  if result
    logger.info("[#{table}] No error on batch #{batch[:name]}, #{count} lines")
  else
    logger.error("[#{table}] Error on batch #{batch[:name]}, #{count} lines")
    Stats.add_error("[#{table}] Errors on batch: #{batch[:name]}")
  end
end

if Stats.all_errors.any?
  logger.error("Errors found: #{Stats.all_errors.count}: #{Stats.all_errors.join(', ')}")
  exit 1
else
  logger.warn("No error found in #{to_do.size} batches, #{Stats.all_lines} lines compared")
end
