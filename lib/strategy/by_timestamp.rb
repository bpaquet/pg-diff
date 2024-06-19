# frozen_string_literal: true

require_relative 'by_id'
require 'time'

module Strategy
  # Diff table by iterating on numerical ids
  class ByTimestamp < ById
    def str_to_key(str)
      return nil if str.nil? || str == '\\N'

      match = str.match(/^now\s*-\s*(\d+)$/)
      return Time.now - match[1].to_i if match

      Time.parse(str)
    end

    def key_to_pg(key)
      "'#{key}'"
    end

    def build_next_key(current, increment)
      # increment is in days
      current + (increment.to_f * 24 * 3600)
    end
  end
end
