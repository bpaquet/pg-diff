# frozen_string_literal: true

require_relative 'base'

module Strategy
  # Diff table in one shot
  class OneShot < Base
    def batches
      [{
        name: "full_#{@table}",
        where: '1 = 1'
      }]
    end
  end
end
