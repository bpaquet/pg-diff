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

  def extract_key(line)
    line[2..].split("\t").first
  end

  def build_output(outs, ins)
    outputs = []
    outputs += outs.map do |key, _|
      ins[key] ? { 'changed' => key } : { 'only_in_source' => key }
    end
    ins.each_key do |key|
      outputs << { 'only_in_target' => key } unless outs[key]
    end
    outputs
  end

  def parse(log_file)
    outs = {}
    ins = {}
    File.readlines(log_file).map(&:strip).each do |line|
      if line.start_with?('< ')
        outs[extract_key(line)] = true
      elsif line.start_with?('> ')
        ins[extract_key(line)] = true
      end
    end
    build_output(outs, ins)
  end

  def process(log_file)
    return unless filename

    diffs = parse(log_file)
    append(diffs.map { |diff| "#{diff.keys.first}: #{diff.values.first}" }.join("\n"))
  end

  def append(content)
    @mutex.synchronize do
      File.write(filename, "#{content}\n", mode: 'a')
    end
  end
end
