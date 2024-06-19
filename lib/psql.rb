# frozen_string_literal: true

require 'tempfile'
require 'logger'
require 'open3'

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
    Open3.popen3("#{@options[:psql]} #{url} -v ON_ERROR_STOP=on -f #{file.path}") do |_, stdout, stderr, wait_thr|
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
end
