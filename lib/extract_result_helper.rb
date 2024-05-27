# frozen_string_literal: true

# Generate a usable log from diff
class ExtractResultHelper
  attr_accessor :filename

  def initialize(options, table)
    @options = options
    @filename = options[:extract_result_to_file]&.gsub('<TABLE>', table)
    @mutex = Mutex.new
    FileUtils.rm_f(@filename) if @filename
  end

  def process(log_file) # rubocop:disable Metrics/PerceivedComplexity
    return unless filename

    outputs = []
    last_key = nil
    File.readlines(log_file).map(&:strip).each do |line|
      if line.start_with?('< ')
        last_key = line[2..].split("\t").first
        outputs << "only_in_source: #{last_key}"
      elsif line.start_with?('> ')
        new_key = line[2..].split("\t").first
        if last_key == new_key
          outputs[-1] = "changed: #{new_key}"
        else
          last_key = new_key
          outputs << "only_in_target: #{line[2..].split("\t").first}"
        end
      end
    end
    append(outputs.join("\n"))
  end

  def append(content)
    @mutex.synchronize do
      File.write(filename, "#{content}\n", mode: 'a')
    end
  end
end
