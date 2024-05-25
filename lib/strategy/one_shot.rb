# frozen_string_literal: true

module Strategy
  # Diff table in one shot
  class OneShot
    def batches
      [{
        name: 'full',
        where: '1 = 1'
      }]
    end
  end
end
