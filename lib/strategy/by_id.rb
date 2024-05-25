# frozen_string_literal: true

require_relative 'base'

module Strategy
  # Diff table by iterating on ids
  class ById < Base
    def _compute_key(suffix, operation, db)
      file = @options[:tmp_dir] + "/#{@table}_src_#{suffix}"
      @psql.run_copy("SELECT #{operation}(#{@options[:key]}) as k FROM #{@table}", file, db)
      result = File.read(file).strip.to_i
      File.unlink(file)
      result
    end

    def compute_key(suffix, operation)
      logger.info("Computing #{operation} key for #{@table}, key: #{@options[:key]}")
      [
        _compute_key("#{suffix}_src", operation, @options[:src]),
        _compute_key("#{suffix}_target", operation, @options[:target])
      ].send(operation.to_sym)
    end

    def key_start
      @key_start ||= @options[:key_start]&.to_i || compute_key('start', 'min')
    end

    def key_stop
      @key_stop ||= @options[:key_stop]&.to_i || (compute_key('stop', 'max') + 1)
    end

    def build_batch(current, next_current)
      {
        name: "#{@table}_#{current}",
        where: "#{@options[:key]} >= #{current} AND #{@options[:key]} < #{next_current}"
      }
    end

    def batches
      logger.info("Key range: #{key_start} - #{key_stop}")
      result = []
      current = key_start
      while current < key_stop
        next_current = current + @options[:batch_size]
        result << build_batch(current, next_current)
        current = next_current
      end
      result
    end
  end
end
