# frozen_string_literal: true

require_relative 'extract_result_helper'

# Handle the comparison of a single table
class TableComparer
  attr_accessor :options, :psql, :table, :target_table, :key, :columns

  def initialize(options, psql, table)
    @options = options
    @psql = psql
    @table = table
    @target_table = options[:table_mapping].gsub('<TABLE>', table)
    @extract_result_helper = ExtractResultHelper.new(options, table)
  end

  def logger
    @logger ||=
      begin
        logger = Logger.new($stdout)
        logger.level = options[:log_level]
        logger
      end
  end

  def configure_columns!
    logger.warn("[#{table}] Configuring columns")

    src_columns = @psql.columns(@table, @options[:src])
    src_columns.select! { |k, _v| options[:columns].include?(k) } if options[:columns]

    @key = options[:key]
    raise("[#{table}] Missing key #{key}") unless src_columns[key]
    raise("[#{table}] Key #{key} is nullable") unless src_columns[key] == 'NO'

    @columns = src_columns.keys
    target_columns = psql.columns(target_table, options[:target]).keys

    if columns & target_columns != columns

      raise("[#{table}] Missing columns in target table #{target_table}: #{columns - target_columns}")
    end

    return unless columns != target_columns

    logger.warn("[#{table}] Different columns in target table #{target_table}: #{target_columns - columns}")
  end

  def compute_batches
    logger.warn("[#{table}] Computing batches")

    strategy_klass = "Strategy::#{options[:strategy].split('_').map(&:capitalize).join}"
    batches = Object.const_get(strategy_klass).new(options, psql, table, target_table).batches
    logger.info("[#{table}] Comparing with #{batches.size} batches, strategy: #{options[:strategy]}")
    logger.info("[#{table}] Key: #{key}, columns: #{columns.join(', ')}")
    batches.map { |batch| [self, batch] }
  end

  def where(batch)
    where_clause = batch[:where]
    if options[:limit_to_the_past_minutes]
      where_key = options[:limit_to_the_past_key] || options[:key]
      where_clause += " AND #{where_key} < '#{Time.now - (options[:limit_to_the_past_minutes] * 60)}'"
    end
    where_clause
  end

  def copy(batch, source_table)
    psql.build_copy(
      "select #{columns.join(', ')} from #{source_table} WHERE #{where(batch)} ORDER BY #{options[:order_by]}"
    )
  end

  def psql_command(file, url)
    "#{options[:psql]} #{url} -v ON_ERROR_STOP=on -f #{file.path}"
  end

  def process_batch(batch)
    src_sql_file = copy(batch, table)
    target_sql_file = copy(batch, target_table)

    diff_file = Tempfile.new("diff_#{table}")
    wc_file = Tempfile.new("wc_#{table}")
    wc_file.close
    command = 'diff --speed-large-file ' \
              "<(#{psql_command(src_sql_file, options[:src])} | tee >(wc -l > #{wc_file.path})) " \
              "<(#{psql_command(target_sql_file, options[:target])} ) " \

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
      @extract_result_helper.process(diff_file.path)
    end
    [src_sql_file, target_sql_file, wc_file, bash_file, diff_file].each(&:unlink)
  end
end