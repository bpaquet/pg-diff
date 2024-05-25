# frozen_string_literal: true

module Strategy
  # Diff table in one shot
  class OneShot
    def initialize(options, table)
      @options = options
      @table = table
    end

    def batches
      [{
        name: "full_#{@table}",
        where: '1 = 1'
      }]
    end
  end
end
