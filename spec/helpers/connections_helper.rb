
require 'aws-sdk-v1'

module GoodData
  module Connectors
    module DownloaderCsv
      module Connections
        class << self
          def init
            @type = 'csv'
            @metadata = GoodData::Connectors::Metadata::Metadata.new(GoodData::Connectors::DownloaderCsv::ConnectionHelper::PARAMS)
            @downloader = GoodData::Connectors::DownloaderCsv::Csv.new(@metadata, GoodData::Connectors::DownloaderCsv::ConnectionHelper::PARAMS)
            @metadata.set_source_context(GoodData::Connectors::DownloaderCsv::ConnectionHelper::DEFAULT_DOWNLOADER, {}, @downloader)
          end

          attr_reader :metadata

          attr_reader :downloader
        end
      end
    end
  end
end
