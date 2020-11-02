# encoding: UTF-8

require 'simplecov'
require 'pmap'
require 'rspec'
require 'pathname'
require 'aws-sdk-v1'
require 'aws-sdk'
require 'gooddata'

require 'spec_helper'
require 'environment/default'

# Automagically include all helpers/*_helper.rb

base = Pathname(__FILE__).dirname.expand_path
Dir.glob(base + 'helpers/*_helper.rb').each do |file|
  require file
end
require 'helpers/validator'
require 'helpers/batch_validator'
require 'helpers/runtime_metadata_validator'

RSpec.configure do |config|
  # config.include SchemaHelper

  config.filter_run_excluding broken: true

  config.include GoodData::Connectors::DownloaderCsv::ConnectionHelper
  config.include GoodData::Connectors::DownloaderCsv::IntegrationHelper
  config.include GoodData::Connectors::DownloaderCsv::S3Helper
  config.include GoodData::Connectors::DownloaderCsv::WebDavHelper
  config.include GoodData::Connectors::DownloaderCsv::SftpHelper
  config.include GoodData::Connectors::DownloaderCsv::Connections

  config.before(:all) do
    $log = Logger.new(STDOUT)
    # $log = Logger.new(File.open(File::NULL, 'w'))
  end

  config.before(:suite) do
    FileUtils.mkdir('tmp') unless Dir.exist?('tmp')

    GoodData::Connectors::DownloaderCsv::S3Helper.connect
    remote_path = GoodData::Connectors::DownloaderCsv::S3Helper.generate_remote_path('configuration.json')
    GoodData::Connectors::DownloaderCsv::S3Helper.upload_file('spec/data/configurations/default_configuration.json', remote_path)
  end

  config.after(:suite) do
    FileUtils.rm_rf('tmp')
    FileUtils.rm_rf('metadata')
    FileUtils.rm_rf('source')
    GoodData::Connectors::DownloaderCsv::S3Helper.clear_bucket(GoodData::Connectors::DownloaderCsv::ConnectionHelper::DEFAULT_TIMESTAMP)
  end
end

SimpleCov.formatter = SimpleCov::Formatter::MultiFormatter[
  SimpleCov::Formatter::HTMLFormatter
]

SimpleCov.start do
  add_group 'Downloader', 'lib/gdc_etl_csv_downloader'
end
