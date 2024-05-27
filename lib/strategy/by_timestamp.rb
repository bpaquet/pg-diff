# frozen_string_literal: true

require_relative 'by_id'
require 'datetime'

module Strategy
  # Diff table by iterating on numerical ids
  class ByTimestamp < ById
    def str_to_key(str)
      return nil if str.nil? || str == '\\N'

      DateTime.parse(str)
    end

    def key_to_pg(key)
      "'#{key}'"
    end

    def build_batch(current, next_current)
      {
        name: "#{@table}_#{current}",
        where: "#{@options[:key]} >= '#{current}' AND #{@options[:key]} < '#{next_current}'"
      }
    end

    def build_next_key(current)
      current + @options[:batch_size].to_f
    end
  end
end
