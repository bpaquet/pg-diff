# frozen_string_literal: true

# Thread-safe statistics collector
module Stats
  def self.add_error(error)
    @mutex_error ||= Mutex.new
    @mutex_error.synchronize do
      @errors ||= []
      @errors << error
    end
  end

  def self.all_errors
    @errors || []
  end

  def self.add_lines(lines)
    @mutex_lines ||= Mutex.new
    @mutex_lines.synchronize do
      @lines ||= 0
      @lines += lines
    end
  end

  def self.all_lines
    @lines || 0
  end
end
