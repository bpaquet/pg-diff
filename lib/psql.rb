# frozen_string_literal: true

require 'tempfile'
require 'logger'

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

  def run_copy(sql_command, file, url)
    Psql.record_sql_file(sql_command, @options[:record_sql_file]) if @options[:record_sql_file]
    run_psql_command("\\copy ( #{sql_command} ) to #{file}", url)
  end

  def run_psql_command(sql_command, url) # rubocop:disable Metrics/MethodLength,Metrics/AbcSize
    logger.debug("Running toward #{url}: #{sql_command}")
    f = Tempfile.new('sql')
    output = Tempfile.new('output')
    f.write(sql_command)
    f.close
    ok = system("#{@options[:psql]} #{url} -v ON_ERROR_STOP=on -f #{f.path} > #{output.path} 2>&1")
    f.unlink
    if ok
      output.unlink
    else
      puts File.read(output.path)
      output.unlink
      raise("Failed to run psql command on #{url}")
    end
  end
end
