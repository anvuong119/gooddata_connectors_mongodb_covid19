
require 'net/dav'

module GoodData
  module Connectors
    module DownloaderCsv
      module WebDavHelper
        class << self
          def connect
            @dav = Net::DAV.new(GoodData::Connectors::DownloaderCsv::ConnectionHelper::WEBDAV_URL, curl: false)
            @dav.verify_server = false
            @dav.credentials(GoodData::Connectors::DownloaderCsv::ConnectionHelper::WEBDAV_USERNAME, GoodData::Connectors::DownloaderCsv::ConnectionHelper::WEBDAV_PASSWORD)
            @dav
          end

          def disconnect
            # do nothing
          end

          def upload(local_source, remote_target, texts_to_replace = {})
            local_source_content = File.open(local_source, 'r').read
            texts_to_replace.each { |old_text, new_text| local_source_content = local_source_content.gsub(old_text, new_text)}
            @dav.start do |dav|
              dav.put_string(remote_target, local_source_content)
            end
          end

          def delete(remote)
            @dav.start do |dav|
              if (dav.exists?(remote))
                dav.delete(remote)
              end
            end
          end
        end
      end
    end
  end
end
