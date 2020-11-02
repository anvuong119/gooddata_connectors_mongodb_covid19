
require 'aws-sdk-v1'
require 'securerandom'

module GoodData
  module Connectors
    module DownloaderCsv
      module S3Helper
        class << self
          def connect
            args = {
              access_key_id: GoodData::Connectors::DownloaderCsv::ConnectionHelper::DEFAULT_BDS_ACCESS_KEY,
              secret_access_key: GoodData::Connectors::DownloaderCsv::ConnectionHelper::DEFAULT_BDS_SECRET_KEY,
              max_retries: 15,
              http_read_timeout: 120,
              http_open_timeout: 120
            }

            @s3 = AWS::S3.new(args)
            @bucket = @s3.buckets[GoodData::Connectors::DownloaderCsv::ConnectionHelper::DEFAULT_BDS_BUCKET]
          end

          def upload_file(from, to, texts_to_replace = {})
            local_source_content = File.open(from, 'r').read
            texts_to_replace.each { |old_text, new_text| local_source_content = local_source_content.gsub(old_text, new_text)}
            obj = @bucket.objects[to]
            obj.write(local_source_content)
          end

          def download_files(from, to)
            FileUtils.mkdir(to) unless Dir.exist?(to)
            list = @bucket.objects.with_prefix(from)
            list.each do |object|
              filename = object.key.split('/').last
              File.open(to + filename, 'w') do |f|
                f.write(object.read)
              end
            end
          end

          def list_files(from)
            list = @bucket.objects.with_prefix(from)
            list.map { |object| object.key.split('/').last }
          end

          def exists?(path)
            @bucket.objects[path].exists?
          end

          def get_object(path)
            @bucket.objects[path]
          end

          def generate_remote_path(file, remote_testing_folder = GoodData::Connectors::DownloaderCsv::ConnectionHelper::DEFAULT_TIMESTAMP)
            File.join(
                GoodData::Connectors::DownloaderCsv::ConnectionHelper::DEFAULT_BDS_FOLDER,
                remote_testing_folder,
                file
            )
          end

          def generate_token_remote_path(remote_testing_folder)
            File.join(
                GoodData::Connectors::DownloaderCsv::ConnectionHelper::DEFAULT_BDS_FOLDER,
                remote_testing_folder
            )
          end

          def copy_data_folder(remote_testing_folder = nil)
            path = 'spec/data/files/data_folder/**/*'
            Dir.glob(path).each do |element|
              unless File.directory?(element)
                remote_path = generate_remote_path((remote_testing_folder) ? File.join(remote_testing_folder + '/data_path', element.split('/')[4..-1].join('/')) : File.join('data_path', element.split('/')[4..-1].join('/')))
                upload_file(element, remote_path)
              end
            end
          end

          def copy_batch_folder(remote_testing_folder = nil)
            path = (remote_testing_folder) ? 'spec/data/files/batches/csv_downloader_1/**/*' : 'spec/data/files/batches/**/*'
            Dir.glob(path).each do |element|
              unless File.directory?(element)
                remote_path = generate_remote_path((remote_testing_folder) ? File.join('batches/' + remote_testing_folder, element.split('/')[5..-1].join('/')) : File.join('batches', element.split('/')[4..-1].join('/')))
                upload_file(element, remote_path)
              end
            end
          end

          def delete_data_folder(remote_testing_folder = nil)
            list = @bucket.objects.with_prefix((remote_testing_folder) ? generate_remote_path(remote_testing_folder + '/data_path') : generate_remote_path('data_path'))
            list.each(&:delete)
          end

          # Delete all contents in token folder on bucket
          def clear_bucket(remote_testing_folder = nil)
            if remote_testing_folder
              @bucket.objects.with_prefix(generate_token_remote_path(remote_testing_folder)).delete_all
            else
              @bucket.objects.with_prefix(generate_remote_path('batches/')).delete_all
              @bucket.objects.with_prefix(generate_remote_path('metadata/')).delete_all
              @bucket.objects.with_prefix(generate_remote_path('csv_downloader_1/')).delete_all
              @bucket.objects.with_prefix(generate_remote_path('data_files/')).delete_all
              @bucket.objects.with_prefix(generate_remote_path('manifests/')).delete_all
              @bucket.objects.with_prefix(generate_remote_path('cache/')).delete_all
            end
          end

		      def generate_testing_folder_name
            'testing_' + Time.now.utc.strftime('%Y_%m_%d_%H_%M_%s') + "_#{SecureRandom.hex(6)}"
          end

          def build_connection_params(testing_folder_name, use_aws_java_sdk = false)
            {
              'bds_bucket' => GoodData::Connectors::DownloaderCsv::ConnectionHelper::DEFAULT_BDS_BUCKET,
              'bds_folder' => GoodData::Connectors::DownloaderCsv::ConnectionHelper::DEFAULT_BDS_FOLDER + '/' + testing_folder_name,
              'bds_access_key' => GoodData::Connectors::DownloaderCsv::ConnectionHelper::DEFAULT_BDS_ACCESS_KEY,
              'bds_secret_key' => GoodData::Connectors::DownloaderCsv::ConnectionHelper::DEFAULT_BDS_SECRET_KEY,
              'csv|options|access_key' => GoodData::Connectors::DownloaderCsv::ConnectionHelper::DEFAULT_BDS_ACCESS_KEY,
              'csv|options|secret_key' => GoodData::Connectors::DownloaderCsv::ConnectionHelper::DEFAULT_BDS_SECRET_KEY,
              'ID' => GoodData::Connectors::DownloaderCsv::ConnectionHelper::DEFAULT_DOWNLOADER,
              'GDC_LOGGER' => Logger.new(STDOUT),
              'use_aws_java_sdk' => use_aws_java_sdk
            }
          end

          def get_batches_folder(filename)
            "#{filename}/#{GoodData::Connectors::DownloaderCsv::ConnectionHelper::DEFAULT_DOWNLOADER}/"
          end

          def get_metadata_folder(filename)
            "#{filename}/"
          end

          def get_data_folder(filename)
            "#{GoodData::Connectors::DownloaderCsv::ConnectionHelper::DEFAULT_DOWNLOADER}/#{filename}/"
          end

          def get_metadata(batch, entity = '')
            parsed_batch = JSON.parse(batch)
            s3object = get_object(parsed_batch['files'].select { |file| file['entity'].match entity.to_s }.first['file'])
            s3object.metadata.to_h.to_hash.to_s
          end
        end
      end
    end
  end
end
