# frozen_string_literal: true

require 'tempfile'
require 'logger'
require 'open3'
require 'uri'

# Help to run psql commands
class Psql
  def self.record_sql_file(sql, file)
    @mutex ||= Mutex.new
    @mutex.synchronize do
      current = File.exist?(file) ? File.read(file) : ''
      File.write(file, current + "#{sql}\n")
    end
  end

  def initialize(options)
    @options = options
    # Load IAM auth module if needed
    require_relative 'iam_auth' if options[:iam_auth_source] || options[:iam_auth_target]
  end

  def logger
    @logger ||=
      begin
        logger = Logger.new($stdout)
        logger.level = @options[:log_level]
        logger
      end
  end

  def build_sql_file(sql_command)
    logger.debug("Building sql file: #{sql_command}")
    file = Tempfile.new('sql')
    file.write(sql_command)
    file.close
    file
  end

  def build_copy(sql_command)
    Psql.record_sql_file(sql_command, @options[:record_sql_file]) if @options[:record_sql_file]
    build_sql_file("\\copy ( #{sql_command} ) to STDOUT")
  end

  def run_psql_file(file, url)
    env = build_env_for_url(url)
    Open3.popen3(env, "#{@options[:psql]} #{url} -v ON_ERROR_STOP=on -f #{file.path}") do |_, stdout, stderr, wait_thr|
      wait_thr.join
      result = wait_thr.value
      file.unlink
      unless result.success?
        puts stdout.read
        puts stderr.read
        raise('Failed to psql command')
      end
      stdout.read
    end
  end

  def build_env_for_url(url)
    env = {}

    # Check if this URL should use IAM authentication
    if should_use_iam_auth?(url)
      password = IamAuth.generate_token(url, logger: logger)
      env['PGPASSWORD'] = password
    end

    env
  end

  def build_env_string_for_url(url)
    # Build environment variable string for shell commands
    env = build_env_for_url(url)
    return '' if env.empty?

    # Properly escape values for shell - replace single quotes with '\''
    env.map { |k, v| "#{k}='#{v.gsub("'", "'\\''")}'" }.join(' ')
  end

  def should_use_iam_auth?(url)
    (@options[:iam_auth_source] && url == @options[:src]) ||
      (@options[:iam_auth_target] && url == @options[:target])
  end

  def columns(table, url)
    file = build_copy(
      'SELECT column_name, is_nullable FROM information_schema.columns ' \
      "where (table_schema || '.' || table_name='#{table}')  " \
      "or (table_schema = 'public' and table_name='#{table}') order by ordinal_position"
    )
    output = run_psql_file(file, url)
    output.split("\n").to_h do |line|
      line.strip.split("\t")
    end
  end

  def column_type(table, url, column)
    file = build_copy(
      'SELECT data_type FROM information_schema.columns ' \
      "where (table_schema || '.' || table_name='#{table}')  " \
      "or (table_schema = 'public' and table_name='#{table}') AND column_name='#{column}' "
    )
    run_psql_file(file, url).strip
  end
end
