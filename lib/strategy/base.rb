# frozen_string_literal: true

module Strategy
  # Bae class for strategies
  class Base
    def initialize(options, psql, table, target_table)
      @options = options
      @psql = psql
      @table = table
      @target_table = target_table
    end

    def logger
      @logger ||=
        begin
          logger = Logger.new($stdout)
          logger.level = @options[:log_level]
          logger
        end
    end
  end
end
