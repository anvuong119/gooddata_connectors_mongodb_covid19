module GoodData
  module Connectors
    module DownloaderCsv
      class BatchValidatorError < StandardError
      end

      class BatchValidator < Validator
        attr_accessor :id, :sequence, :filename, :files, :file_format

        def initialize(id,sequence,filename,files, file_format = 'csv')
          self.id = id
          self.sequence = sequence
          self.filename = filename
          self.files = files
          self.file_format = file_format
        end


        def validate(batch)
          batch = JSON.parse(batch)
          raise BatchValidatorError, 'Identification is invalid' unless batch['identification'] == id
          raise BatchValidatorError, 'Sequence is invalid' unless batch['sequence'] == sequence
          raise BatchValidatorError, 'Filename is invalid' unless batch['filename'].match filename

          self.files.each do |entity, count|
            entity_files = batch['files'].select{|file| file['entity'] == entity}
            raise BatchValidatorError, "Missing files for entity #{entity}" unless entity_files.length == count
            entity_files.each do |file|
              raise BatchValidatorError, "File #{file} has wrong path" unless file['file'].match %r(AIDAJFITXNOA7ZNFS3H4U_gdc\-ms\-connectors_ConnectorsTestSuite\/testing\/unit\/csv_downloader_1\/#{entity}\/#{Regexp.quote(time_path)}\/\d{10}_data_.{8}\.#{file_format})
            end
          end

          true

        rescue BatchValidatorError => e
          puts "BatchValidatorError: #{e}"
          false
        end

      end
    end
  end
end