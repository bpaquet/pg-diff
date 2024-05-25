# frozen_string_literal: true

# Thread-safe error collection
module Errors
  def self.add(error)
    @mutex ||= Mutex.new
    @mutex.synchronize do
      @errors ||= []
      @errors << error
    end
  end

  def self.all
    @errors || []
  end
end
