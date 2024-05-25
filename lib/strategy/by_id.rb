# frozen_string_literal: true

require_relative 'base'

module Strategy
  # Diff table by iterating on numerical ids
  class ById < Base
    def _compute_key(suffix, operation, db, table)
      file = @options[:tmp_dir] + "/#{@table}_src_#{suffix}"
      @psql.run_copy("SELECT #{operation}(#{@options[:key]}) as k FROM #{table}", file, db)
      result = str_to_key(File.read(file).strip)
      File.unlink(file)
      result
    end

    def compute_key(suffix, operation)
      logger.info("Computing #{operation} key for #{@table}, key: #{@options[:key]}")
      [
        _compute_key("#{suffix}_src", operation, @options[:src], @table),
        _compute_key("#{suffix}_target", operation, @options[:target], @target_table)
      ].send(operation.to_sym)
    end

    def str_to_key(str)
      str&.to_i
    end

    def key_start
      @key_start ||= str_to_key(@options[:key_start]) || compute_key('start', 'min')
    end

    def key_stop
      @key_stop ||=  str_to_key(@options[:key_stop]) || (compute_key('stop', 'max') + 1)
    end

    def build_batch(current, next_current)
      {
        name: "#{@table}_#{current}",
        where: "#{@options[:key]} >= #{current} AND #{@options[:key]} < #{next_current}"
      }
    end

    def build_next_key(current)
      current + @options[:batch_size]
    end

    def batches
      logger.info("Key range: #{key_start} - #{key_stop}")
      result = []
      current = key_start
      while current < key_stop
        next_current = build_next_key(current)
        result << build_batch(current, next_current)
        current = next_current
      end
      result
    end
  end
end
