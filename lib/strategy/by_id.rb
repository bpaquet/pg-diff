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
      logger.info("[#{@table}] Computing #{operation} for key: #{@options[:key]}")
      Parallel.map([
                     ["#{suffix}_src", operation, @options[:src], @table],
                     ["#{suffix}_target", operation, @options[:target], @target_table]
                   ], in_threads: 2) do |local_suffix, local_operation, db, table|
        _compute_key(local_suffix, local_operation, db, table)
      end.send(operation.to_sym)
    end

    def str_to_key(str)
      str&.to_i
    end

    def key_start
      @key_start ||= str_to_key(@options[:key_start]) || compute_key('start', 'min')
    end

    def key_stop
      @key_stop ||= str_to_key(@options[:key_stop]) || compute_key('stop', 'max')
    end

    def key_to_pg(key)
      key
    end

    def build_batch(current, next_current)
      {
        name: "#{@table}_#{current}",
        where: "#{@options[:key]} >= #{key_to_pg(current)} AND #{@options[:key]} < #{key_to_pg(next_current)}"
      }
    end

    def build_next_key(current)
      current + @options[:batch_size].to_i
    end

    def empty_batch
      {
        name: "empty_#{@table}",
        where: '1 = 1'
      }
    end

    def batches # rubocop:disable Metrics/AbcSize,Metrics/MethodLength
      # Precompute key_start and key_stop
      Parallel.each(%i[key_start key_stop], in_threads: 2) { |method| send(method) }

      logger.info("[#{@table}] Key range: #{key_start} - #{key_stop}")
      result = []
      return [empty_batch] if key_start.nil? || key_stop.nil?

      current = key_start
      while current < (key_stop + 1)
        next_current = build_next_key(current)
        result << build_batch(current, next_current)
        current = next_current
      end
      result
    end
  end
end
