# frozen_string_literal: true

require_relative 'by_id'
require 'time'

module Strategy
  # Diff table by iterating on numerical ids
  class ByTimestamp < ById
    def str_to_key(str)
      return nil if str.nil? || str == '\\N'

      Time.parse(str)
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

    def build_next_key(current, increment)
      # increment is in days
      current + (increment.to_f * 24 * 3600)
    end
  end
end
