# encoding: UTF-8

require 'gdc_etl_csv_downloader'
require 'execute_brick'

describe GoodData::Connectors::DownloaderCsv::CsvDownloaderMiddleWare do

  before :all do
    Csv = GoodData::Connectors::DownloaderCsv::Csv
    Metadata = GoodData::Connectors::Metadata::Metadata
    ConnectionHelper = GoodData::Connectors::DownloaderCsv::ConnectionHelper
    IntegrationHelper = GoodData::Connectors::DownloaderCsv::IntegrationHelper
    S3Helper = GoodData::Connectors::DownloaderCsv::S3Helper
    LOCAL_TESTING_FOLDER = S3Helper.generate_testing_folder_name.freeze
    REMOTE_TESTING_FOLDER = S3Helper.generate_testing_folder_name.freeze
    CONNECTION_PARAMS = S3Helper.build_connection_params(REMOTE_TESTING_FOLDER)
    MANIFEST_OPTIONS = {
      '$REMOTE_TESTING_FOLDER' => REMOTE_TESTING_FOLDER
    }
  end

  before :each do
    FileUtils.mkdir(LOCAL_TESTING_FOLDER) unless Dir.exist?(LOCAL_TESTING_FOLDER)
    @padding = 'LeQTVzJG'
    allow(SecureRandom).to receive(:urlsafe_base64).with(6).and_return(@padding)
  end

  after :each do
    FileUtils.rm_rf(LOCAL_TESTING_FOLDER)
    GoodData::Connectors::DownloaderCsv::S3Helper.clear_bucket(REMOTE_TESTING_FOLDER)
    remote_path = S3Helper.generate_token_remote_path(REMOTE_TESTING_FOLDER)
    files = S3Helper.list_files(remote_path)
    while (files.size > 0)
      puts "Deleting testing folder: path=#{remote_path} remaining=#{files.size}"
      sleep 1
      files = S3Helper.list_files(remote_path)
    end
  end

  it 'Is defined' do
    expect(GoodData::Connectors::DownloaderCsv::CsvDownloaderMiddleWare).to be_truthy
  end

  it 'Run downloader from execute brick' do
    # Prepare testing data
    prepare_data_test(REMOTE_TESTING_FOLDER)

    # Run downloader from execute brick
    CONNECTION_PARAMS["csv_downloader_wrapper"] = Csv.new(Metadata.new(CONNECTION_PARAMS), CONNECTION_PARAMS)
    execute_brick = GoodData::Bricks::ExecuteBrick.new
    execute_brick.call(CONNECTION_PARAMS)

    # Verify data
    verify_data(LOCAL_TESTING_FOLDER, REMOTE_TESTING_FOLDER, 'spec/data/files/feature/expected_metadata.json', 'spec/data/files/feature/events.csv')
  end

  it 'Run many times downloader from execute brick' do
    # Prepare testing data
    remote_config_path = S3Helper.generate_remote_path('configuration.json', REMOTE_TESTING_FOLDER)
    S3Helper.upload_file('spec/data/configurations/s3/configuration_9.json', remote_config_path, MANIFEST_OPTIONS)
    remote_data_path = S3Helper.generate_remote_path('data_files/events_1.0_20170903.csv', REMOTE_TESTING_FOLDER)
    S3Helper.upload_file('spec/data/files/feature/events.csv', remote_data_path)
    feed_path = S3Helper.generate_remote_path('feed', REMOTE_TESTING_FOLDER)
    S3Helper.upload_file('spec/data/files/feature/feed', feed_path)
    old_batch = []
    old_data = []
    old_metadata = []

    # Run first downloader from execute brick
    CONNECTION_PARAMS["csv_downloader_wrapper"] = Csv.new(Metadata.new(CONNECTION_PARAMS), CONNECTION_PARAMS)
    execute_brick = GoodData::Bricks::ExecuteBrick.new
    execute_brick.call(CONNECTION_PARAMS)

    # Verify data
    verify_data(LOCAL_TESTING_FOLDER, REMOTE_TESTING_FOLDER, 'spec/data/files/feature/expected_metadata_5.json', 'spec/data/files/feature/events.csv', old_batch, old_data, old_metadata)

    remote_data_path = S3Helper.generate_remote_path('data_files/events_1.0_20170904.csv', REMOTE_TESTING_FOLDER)
    S3Helper.upload_file('spec/data/files/feature/events_2.csv', remote_data_path)

    # Run second downloader from execute brick
    CONNECTION_PARAMS["csv_downloader_wrapper"] = Csv.new(Metadata.new(CONNECTION_PARAMS), CONNECTION_PARAMS)
    execute_brick = GoodData::Bricks::ExecuteBrick.new
    execute_brick.call(CONNECTION_PARAMS)
    # Verify data
    verify_data(LOCAL_TESTING_FOLDER, REMOTE_TESTING_FOLDER, 'spec/data/files/feature/expected_metadata_5.json', 'spec/data/files/feature/events_2.csv', old_batch, old_data, old_metadata)

  end

  it 'Run downloader from execute brick with two version data' do
    # Prepare testing data
    remote_config_path = S3Helper.generate_remote_path('configuration.json', REMOTE_TESTING_FOLDER)
    S3Helper.upload_file('spec/data/configurations/s3/configuration_9.json', remote_config_path, MANIFEST_OPTIONS)
    remote_data_path = S3Helper.generate_remote_path('data_files/events_1.1_20170903.csv', REMOTE_TESTING_FOLDER)
    S3Helper.upload_file('spec/data/files/feature/events.csv', remote_data_path)
    remote_data_path = S3Helper.generate_remote_path('data_files/events_1.2_20170904.csv', REMOTE_TESTING_FOLDER)
    S3Helper.upload_file('spec/data/files/feature/events0.csv', remote_data_path)
    remote_data_path = S3Helper.generate_remote_path('data_files/events_1.3_20170904.csv', REMOTE_TESTING_FOLDER)
    S3Helper.upload_file('spec/data/files/feature/events1.csv', remote_data_path)
    feed_path = S3Helper.generate_remote_path('feed', REMOTE_TESTING_FOLDER)
    S3Helper.upload_file('spec/data/files/feature_metadata_caching/feed2', feed_path)

    begin
      # Run first downloader from execute brick
      CONNECTION_PARAMS["csv_downloader_wrapper"] = Csv.new(Metadata.new(CONNECTION_PARAMS), CONNECTION_PARAMS)
      execute_brick = GoodData::Bricks::ExecuteBrick.new
      execute_brick.call(CONNECTION_PARAMS)
    rescue RuntimeError => e
      expect(e.message).to eq 'The manifest file contain more then one version of entity. This is not supported in current version of CSV downloader'
    end
  end

  it 'Run many times downloader from execute brick with partial full load' do
    # Prepare testing data
    remote_config_path = S3Helper.generate_remote_path('configuration.json', REMOTE_TESTING_FOLDER)
    S3Helper.upload_file('spec/data/configurations/s3/configuration_10.json', remote_config_path, MANIFEST_OPTIONS)
    remote_data_path = S3Helper.generate_remote_path('data_files/events_1.0_20170903.csv', REMOTE_TESTING_FOLDER)
    S3Helper.upload_file('spec/data/files/feature/events.csv', remote_data_path)
    feed_path = S3Helper.generate_remote_path('feed', REMOTE_TESTING_FOLDER)
    S3Helper.upload_file('spec/data/files/feature/feed', feed_path)
    old_batch = []
    old_data = []
    old_metadata = []

    # Run first downloader from execute brick
    CONNECTION_PARAMS["csv_downloader_wrapper"] = Csv.new(Metadata.new(CONNECTION_PARAMS), CONNECTION_PARAMS)
    execute_brick = GoodData::Bricks::ExecuteBrick.new
    execute_brick.call(CONNECTION_PARAMS)

    # Verify data
    verify_data(LOCAL_TESTING_FOLDER, REMOTE_TESTING_FOLDER, 'spec/data/files/feature/expected_metadata_9.json', 'spec/data/files/feature/events.csv', old_batch, old_data, old_metadata)

    remote_data_path = S3Helper.generate_remote_path('data_files/events_1.0_20170904.csv', REMOTE_TESTING_FOLDER)
    S3Helper.upload_file('spec/data/files/feature/events_partial1.csv', remote_data_path)
    remote_data_path = S3Helper.generate_remote_path('data_files/events_1.0_20170905.csv', REMOTE_TESTING_FOLDER)
    S3Helper.upload_file('spec/data/files/feature/events_partial2.csv', remote_data_path)

    # Run second downloader from execute brick
    CONNECTION_PARAMS["csv_downloader_wrapper"] = Csv.new(Metadata.new(CONNECTION_PARAMS), CONNECTION_PARAMS)
    execute_brick = GoodData::Bricks::ExecuteBrick.new
    execute_brick.call(CONNECTION_PARAMS)
    # Verify data
    expected_batch_pattern = %r(\{\n\ \ "identification":\ "csv_downloader_1",\n\ \ "sequence":\ null,\n\ \ "filename":\ "manifest\-#{Time.now.strftime("%Y") + Time.now.strftime("%m") + Time.now.strftime("%d")}\d{6}",\n\ \ "files":\ \[\n\ \ \ \ \{\n\ \ \ \ \ \ "entity":\ "Event",\n\ \ \ \ \ \ "file":\ "ETL_tests\/connectors_downloader_csv\/#{REMOTE_TESTING_FOLDER}\/#{ConnectionHelper::DEFAULT_DOWNLOADER}\/Event\/#{Regexp.quote(IntegrationHelper.time_path)}\/\d{16}_data_#{@padding}\.csv"\n\ \ \ \ \},\n\ \ \ \ \{\n\ \ \ \ \ \ "entity":\ "Event",\n\ \ \ \ \ \ "file":\ "ETL_tests\/connectors_downloader_csv\/#{REMOTE_TESTING_FOLDER}\/#{ConnectionHelper::DEFAULT_DOWNLOADER}\/Event\/#{Regexp.quote(IntegrationHelper.time_path)}\/\d{16}_data_#{@padding}\.csv"\n\ \ \ \ \}\n\ \ \]\n\})
    verify_data(LOCAL_TESTING_FOLDER, REMOTE_TESTING_FOLDER, 'spec/data/files/feature/expected_metadata_9.json', nil, old_batch, old_data, old_metadata, expected_batch_pattern)

  end

  # Run directly from main.rb need config GDC_HOSTNAME, GDC_USERNAME, GDC_PASSWORD and GDC_PROJECT_ID parameters for
  # GoodDataCustomMiddleware, the parameters don't using for CSV Downloader so ignore the test.
  # it 'Run downloader from main' do
  #   # Prepare testing data
  #   prepare_data_test(REMOTE_TESTING_FOLDER)
  #
  #   # Run downloader from main.rb
  #   $SCRIPT_PARAMS = CONNECTION_PARAMS.clone
  #   require './main.rb'
  #
  #   # Verify data
  #   verify_data(LOCAL_TESTING_FOLDER, REMOTE_TESTING_FOLDER)
  # end

  def prepare_data_test(remote_folder)
    remote_config_path = S3Helper.generate_remote_path('configuration.json', remote_folder)
    S3Helper.upload_file('spec/data/configurations/s3/configuration_1.json', remote_config_path, MANIFEST_OPTIONS)
    remote_data_path = S3Helper.generate_remote_path('data_files/events.csv', remote_folder)
    S3Helper.upload_file('spec/data/files/feature/events.csv', remote_data_path)
  end

  def verify_data(local_folder, remote_folder, expected_file_metadata, expected_file_data, old_batch = nil, old_data = nil, old_metadata = nil, expected_batch_pattern = nil)
    if expected_batch_pattern.nil?
      expected_batch_pattern = %r(\{\n\ \ "identification":\ "csv_downloader_1",\n\ \ "sequence":\ null,\n\ \ "filename":\ "manifest\-#{Time.now.strftime("%Y") + Time.now.strftime("%m") + Time.now.strftime("%d")}\d{6}",\n\ \ "files":\ \[\n\ \ \ \ \{\n\ \ \ \ \ \ "entity":\ "Event",\n\ \ \ \ \ \ "file":\ "ETL_tests\/connectors_downloader_csv\/#{  remote_folder}\/#{ConnectionHelper::DEFAULT_DOWNLOADER}\/Event\/#{Regexp.quote(IntegrationHelper.time_path)}\/\d{16}_data_#{@padding}\.csv"\n\ \ \ \ \}\n\ \ \]\n\})
    end
    expected_metadata = IntegrationHelper.loadMetadataFile(expected_file_metadata)

    batch_path = S3Helper.generate_remote_path(S3Helper.get_batches_folder('batches'), remote_folder) + IntegrationHelper.time_path
    S3Helper.download_files(batch_path, "#{local_folder}/")
    metadata_path = S3Helper.generate_remote_path(S3Helper.get_metadata_folder('metadata/Event'), remote_folder) + IntegrationHelper.time_path
    S3Helper.download_files(metadata_path, "#{local_folder}/Event/")
    files_path = S3Helper.generate_remote_path(S3Helper.get_data_folder('Event'), remote_folder) + IntegrationHelper.time_path
    S3Helper.download_files(files_path, "#{local_folder}/Event/")

    new_batch = process_new_file(Dir["#{local_folder}/*_batch.json"], old_batch)
    batch = File.open(new_batch).read
    new_data = process_new_file(Dir["#{local_folder}/Event/*_data_#{@padding}.csv"], old_data)
    data = File.open(new_data).read
    new_metadata = process_new_file(Dir["#{local_folder}/Event/*_metadata.json"], old_metadata)
    metadata = File.open(new_metadata).read
    old_batch << new_batch unless old_batch.nil?
    old_data << new_data unless old_data.nil?
    old_metadata << new_metadata unless old_metadata.nil?

    expect(batch).to match(expected_batch_pattern)
    expect(data).to eq File.open(expected_file_data).read unless expected_file_data.nil?
    expect(metadata).to eq expected_metadata
  end

  def process_new_file(list, old_file)
    return list.first if old_file.nil?
    list.each do |file|
      exist = false
      old_file.each do |item|
        exist = true if item == file
      end
      return file unless exist
    end
    list.first
  end
end