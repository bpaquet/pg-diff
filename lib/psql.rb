# frozen_string_literal: true

require 'tempfile'
require 'logger'

# Help to run psql commands
class Psql
  def initialize(options)
    @options = options
  end

  def logger
    @logger ||=
      begin
        logger = Logger.new($stdout)
        logger.level = Logger.const_get(@options[:log_level].upcase)
        logger
      end
  end

  def run_psql_command(sql_command, url)
    logger.info("Running toward #{url}: #{sql_command}")
    f = Tempfile.new('sql')
    output = Tempfile.new('output')
    f.write(sql_command)
    f.close
    cmd = "#{@options[:psql]} #{url} -v ON_ERROR_STOP=on -f #{f.path} > #{output.path} 2>&1"
    failed = system(cmd)
    f.unlink
    output.unlink
    raise("Failed to run #{cmd}") unless failed
  end
end
