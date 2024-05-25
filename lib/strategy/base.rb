# frozen_string_literal: true

module Strategy
  # Bae class for strategies
  class Base
    def initialize(options, psql, table)
      @options = options
      @table = table
      @psql = psql
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
