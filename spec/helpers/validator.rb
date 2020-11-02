module GoodData
  module Connectors
    module DownloaderCsv

      class Validator

        def time_path
          Time.now.strftime('%Y') + '/' + Time.now.strftime('%m') + '/' + Time.now.strftime('%d')
        end
      end
    end
  end
end