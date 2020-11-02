

module GoodData
  module Connectors
    module DownloaderCsv
      module IntegrationHelper
        class << self
          def time_path
            Time.now.utc.strftime('%Y') + '/' + Time.now.utc.strftime('%m') + '/' + Time.now.utc.strftime('%d')
          end

          def execute(downloader, metadata)
            downloader.connect
            downloader.prepare_manifests

            pos = 0
            downloader.number_of_manifest_in_one_run.times do |i|
              puts "Starting processing number #{i + 1}"
              manifest = downloader.load_last_unprocessed_manifest(pos)
              break unless manifest
              entities = metadata.get_downloader_entities_ids
              entities.each do |entity|
                downloader.download_entity_data(entity)
              end
              downloader.load_data_structure_file
              entities.each do |entity|
                downloader.load_metadata(entity)
              end
              downloader.download_all_data
              previous_batch = downloader.finish_load
              if previous_batch.nil?
                pos += 1
                next
              end
              downloader.prepare_next_load(previous_batch)
            end
          end

          def loadMetadataFile(path)
            expectedMetadata = File.open(path).read
            expectedMetadata.gsub!("${metadata_version}") { |match| GoodData::Connectors::Metadata::VERSION }
          end
        end
      end
    end
  end
end