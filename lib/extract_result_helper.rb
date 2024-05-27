# frozen_string_literal: true

# Generate a usable log from diff
module ExtractResultHelper
  def self.cleanup(output_file)
    return unless output_file

    FileUtils.rm_f(output_file)
  end

  def self.process(log_file, output_file) # rubocop:disable Metrics/AbcSize,Metrics/MethodLength,Metrics/CyclomaticComplexity,Metrics/PerceivedComplexity
    return unless output_file

    @mutex_extract_result_to_file ||= Mutex.new
    outputs = []
    last_key = nil
    File.readlines(log_file).map(&:strip).each do |line|
      if line.start_with?('< ')
        last_key = line[2..].split("\t").first
        outputs << "Only in source: #{last_key}"
      elsif line.start_with?('> ')
        new_key = line[2..].split("\t").first
        if last_key == new_key
          outputs[-1] = "Changed: #{new_key}"
        else
          last_key = new_key
          outputs << "Only in destination: #{line[2..].split("\t").first}"
        end
      end
    end
    @mutex_extract_result_to_file.synchronize do
      File.write(output_file, "#{outputs.join("\n")}\n", mode: 'a')
    end
  end
end
