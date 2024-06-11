# frozen_string_literal: true

require_relative 'base'

module Strategy
  # Diff table by iterating on numerical ids
  class ById < Base
    def _compute_key(operation, db, table)
      file = @psql.build_copy("SELECT #{operation}(#{@options[:key]}) as k FROM #{table}")
      result = @psql.run_psql_file(file, db).strip
      str_to_key(result)
    end

    def compute_key(operation)
      logger.info("[#{@table}] Computing #{operation} for key: #{@options[:key]}")
      Parallel.map([
                     [operation, @options[:src], @table],
                     [operation, @options[:target], @target_table]
                   ], in_threads: 2) do |local_operation, db, table|
        _compute_key(local_operation, db, table)
      end.send(operation.to_sym)
    end

    def str_to_key(str)
      str&.to_i
    end

    def key_start
      @key_start ||= str_to_key(@options[:key_start]) || compute_key('min')
    end

    def key_stop
      @key_stop ||= str_to_key(@options[:key_stop]) || compute_key('max')
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

    def build_next_key(current, increment)
      current + increment.to_i
    end

    def empty_batch
      {
        name: "empty_#{@table}",
        where: '1 = 1'
      }
    end

    def batches
      # Precompute key_start and key_stop in parallel
      Parallel.each(%i[key_start key_stop], in_threads: 2) { |method| send(method) }

      logger.info("[#{@table}] Key range: #{key_start} - #{key_stop}")
      result = []
      return [empty_batch] if key_start.nil? || key_stop.nil?

      current = key_start
      while current < build_next_key(key_stop, @options[:batch_size])
        next_current = build_next_key(current, @options[:batch_size])
        end_batch =  @options[:key_stop] ? [next_current, str_to_key(@options[:key_stop])].min : next_current
        result << build_batch(current, end_batch)
        current = next_current
      end
      logger.info("[#{@table}] First batch: #{result.first[:where]}, last batch: #{result.last[:where]}")
      result
    end
  end
end
