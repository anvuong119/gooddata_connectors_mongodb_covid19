module GoodData
  module Connectors
    module DownloaderCsv
      class RuntimeMetadataValidatorError < StandardError
      end

      class RuntimeMetadataValidator < Validator
        attr_accessor :options

        def initialize(options)
          self.options = options
        end

        def validate(metadata)
          values = JSON.parse(metadata['global'])['value']

          options['eq_options'].each do |k,v|
            raise RuntimeMetadataValidatorError, "#{k} is invalid, expected #{v}, got #{values[k]}" unless values[k] == v
          end

          options['match_options'].each do |k,v|
            raise RuntimeMetadataValidatorError, "#{k} is invalid, expected #{values[k]} to match #{v}" unless values[k].match v
          end

          raise RuntimeMetadataValidatorError, 'Invalid metadata_date year' unless values['metadata_date']['year'] == Time.now.strftime('%Y').to_s
          raise RuntimeMetadataValidatorError, 'Invalid metadata_date month' unless values['metadata_date']['month'] == Time.now.strftime('%m').to_s
          raise RuntimeMetadataValidatorError, 'Invalid metadata_date day' unless values['metadata_date']['day'] == Time.now.strftime('%d').to_s
          raise RuntimeMetadataValidatorError, 'Invalid metadata_date timestamp' unless values['metadata_date']['timestamp'].match %r(\d{10})

          true

        rescue RuntimeMetadataValidatorError => e
          puts "RuntimeMetadataValidatorError: #{e}"
          false
        end

      end
    end
  end
end
