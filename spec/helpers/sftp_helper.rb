
require 'gooddata_connectors_backends'

module GoodData
  module Connectors
    module DownloaderCsv
      module SftpHelper
        class << self
          def connect
            @ssh = com.jcraft.jsch.JSch.new
            @session = @ssh.getSession(GoodData::Connectors::DownloaderCsv::ConnectionHelper::SFTP_USERNAME, GoodData::Connectors::DownloaderCsv::ConnectionHelper::SFTP_HOST, 22)
            @session.setConfig('StrictHostKeyChecking', 'no')
            @session.setConfig('compression.s2c', 'none,zlib')
            @session.setConfig('compression.c2s', 'none,zlib')
            @session.setPassword(GoodData::Connectors::DownloaderCsv::ConnectionHelper::SFTP_PASSWORD)
            @session.connect
            @channel = @session.openChannel('sftp')
            @channel.connect
            @sftp = @channel.to_java(com.jcraft.jsch.ChannelSftp)
            @sftp.cd('.')
            @sftp
          end

          def disconnect
            @channel.disconnect unless @channel.nil?
            @session.disconnect unless @session.nil?
          end

          def upload(local_source, remote_target, texts_to_replace = {})
            # replace variable in local_source if any
            local_source_content = File.open(local_source, 'r').read
            texts_to_replace.each { |old_text, new_text| local_source_content = local_source_content.gsub(old_text, new_text)}
            local_name = local_source.split('/').last
            tmp_file = "tmp/#{local_name}"
            File.write(tmp_file, local_source_content)

            # create remote directories if necessary
            remote_dir = remote_target.split('/')[0...-1].join('/')
            mkdir(remote_dir)

            # upload
            @sftp.put(tmp_file, remote_target)
          end

          def mkdir(remote_dir)
            cur_remote_dir = ''
            remote_dir.split('/').each { |dir|
              new_remote_dir = cur_remote_dir.empty? ? dir : "#{cur_remote_dir}/#{dir}"
              begin
                @sftp.mkdir(new_remote_dir)
              rescue
                puts "Remote #{new_remote_dir} already exist"
              end
              cur_remote_dir = new_remote_dir
            }
          end

          def rmdir(remote_dir)
            begin
              @sftp.ls(remote_dir).map do |element|
                if (element.getFilename == '.')
                  next
                end
                if (element.getFilename == '..')
                  next
                end
                filename = remote_dir + '/' + element.getFilename
                attributes = element.getAttrs
                if attributes.isDir
                  rmdir(filename)
                else
                  @sftp.rm(filename)
                end
              end
              @sftp.rmdir(remote_dir)
              puts "Deleted #{remote_dir}."
            rescue
              puts "Remote #{remote_dir} is not exist."
            end
          end
        end
      end
    end
  end
end