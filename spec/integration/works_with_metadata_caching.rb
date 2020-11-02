require 'gdc_etl_csv_downloader'

describe 'Loading metadata with caching', order: :defined do
  before :all do
    Csv = GoodData::Connectors::DownloaderCsv::Csv
    Metadata = GoodData::Connectors::Metadata::Metadata
    ConnectionHelper = GoodData::Connectors::DownloaderCsv::ConnectionHelper
    S3Helper = GoodData::Connectors::DownloaderCsv::S3Helper
    REMOTE_TESTING_FOLDER = S3Helper.generate_testing_folder_name.freeze
    CONNECTION_PARAMS = S3Helper.build_connection_params(REMOTE_TESTING_FOLDER)
    RuntimeMetadataValidator = GoodData::Connectors::DownloaderCsv::RuntimeMetadataValidator
    BatchValidator = GoodData::Connectors::DownloaderCsv::BatchValidator
    MANIFEST_OPTIONS = {
        '$REMOTE_TESTING_FOLDER' => REMOTE_TESTING_FOLDER
    }
    METADATA_OPTIONS = {
        '${metadata_version}' => GoodData::Connectors::Metadata::VERSION
    }
    DEFAULT_RUNTIME_METADATA_OPTIONS = {
        'eq_options' => {
            'downloader_id' => 'csv_downloader_1',
            'manifest_version' => 'default',
            'hash' => 'unknown'
        },
        'match_options' => {
            'metadata_file' => %r(\d{16}_metadata\.json),
            'batch' => %r(\d{16}__batch\.json),
        }
    }.freeze
  end

  before :each do
    FileUtils.mkdir('tmp') unless Dir.exist?('tmp')
  end

  before :each do
    cleanup
  end

  after :each do
    cleanup
  end

  context 'With versioning' do
    it 'Creates metadata cache in the first run and then uses it' do
      # Setup
      remote_config_path = S3Helper.generate_remote_path('configuration.json', REMOTE_TESTING_FOLDER)
      S3Helper.upload_file('spec/data/configurations/s3/configuration_2.json', remote_config_path, MANIFEST_OPTIONS)

      remote_data_path = S3Helper.generate_remote_path('data_files/events.gz', REMOTE_TESTING_FOLDER)
      S3Helper.upload_file('spec/data/files/feature_metadata_caching/events.gz', remote_data_path)

      feed_path = S3Helper.generate_remote_path('feed', REMOTE_TESTING_FOLDER)
      S3Helper.upload_file('spec/data/files/feature_metadata_caching/feed', feed_path)

      manifest_path = S3Helper.generate_remote_path('manifests/manifest_1.20160714105300.csv', REMOTE_TESTING_FOLDER)
      S3Helper.upload_file('spec/data/files/feature_metadata_caching/manifest_1.20160714105300.csv', manifest_path, MANIFEST_OPTIONS)

      @metadata = Metadata.new(CONNECTION_PARAMS)
      @downloader = Csv.new(@metadata, CONNECTION_PARAMS)

      # Execute
      @downloader.execute(CONNECTION_PARAMS)

      expected_cache = expected_cache_object('Event-1.1')

      S3Helper.download_files(S3Helper.generate_remote_path('cache/cache_latest_metadata.json', REMOTE_TESTING_FOLDER), 'tmp/')
      actual_cache = JSON.parse(File.read('tmp/cache_latest_metadata.json'))
      expect(actual_cache).to have_key('Event-1.1')
      actual_cache.delete('update_at')
      actual_cache['Event-1.1'].delete('timestamp')

      expect(actual_cache).to eq expected_cache
    end

    it 'Finds latest metadata for older uncached files' do
      remote_config_path = S3Helper.generate_remote_path('configuration.json', REMOTE_TESTING_FOLDER)
      S3Helper.upload_file('spec/data/configurations/s3/configuration_2.json', remote_config_path, MANIFEST_OPTIONS)

      remote_data_path = S3Helper.generate_remote_path('data_files/events.gz', REMOTE_TESTING_FOLDER)
      S3Helper.upload_file('spec/data/files/feature_metadata_caching/events.gz', remote_data_path)

      # Load processed manifests and metadata
      S3Helper.upload_file('spec/data/files/feature_metadata_caching/manifest_1.20160714105300.csv', S3Helper.generate_remote_path('manifests/processed/manifest_1.20160714105300.csv', REMOTE_TESTING_FOLDER), MANIFEST_OPTIONS)
      S3Helper.upload_file('spec/data/files/feature_metadata_caching/manifest_2.20160714105300.csv', S3Helper.generate_remote_path('manifests/processed/manifest_2.20160714105300.csv', REMOTE_TESTING_FOLDER), MANIFEST_OPTIONS)
      S3Helper.upload_file('spec/data/files/feature_metadata_caching/manifest_3.20160714105300.csv', S3Helper.generate_remote_path('manifests/processed/manifest_3.20160714105300.csv', REMOTE_TESTING_FOLDER), MANIFEST_OPTIONS)
      S3Helper.upload_file('spec/data/files/feature_metadata_caching/Event_1526458589_metadata.json', S3Helper.generate_remote_path('metadata/Event/2018/05/16/1526458589_metadata.json', REMOTE_TESTING_FOLDER), METADATA_OPTIONS)
      S3Helper.upload_file('spec/data/files/feature_metadata_caching/Event_1526458599_metadata.json', S3Helper.generate_remote_path('metadata/Event/2018/05/16/1526458599_metadata.json', REMOTE_TESTING_FOLDER), METADATA_OPTIONS)
      S3Helper.upload_file('spec/data/files/feature_metadata_caching/Event_1526458699_metadata.json', S3Helper.generate_remote_path('metadata/Event/2018/05/16/1526458699_metadata.json', REMOTE_TESTING_FOLDER), METADATA_OPTIONS)
      S3Helper.upload_file('spec/data/files/feature_metadata_caching/feed2', S3Helper.generate_remote_path('feed', REMOTE_TESTING_FOLDER))

      @metadata = Metadata.new(CONNECTION_PARAMS)
      @downloader = Csv.new(@metadata, CONNECTION_PARAMS)

      3.times { @downloader.execute(CONNECTION_PARAMS) }
      # Expect empty cache, no manifests were processed
      S3Helper.download_files(S3Helper.generate_remote_path('cache/cache_latest_metadata.json', REMOTE_TESTING_FOLDER), 'tmp/')
      expect(File).not_to exist('tmp/cache_latest_metadata.json')

      # Add unprocessed manifest linking to the old metadata
      S3Helper.upload_file('spec/data/files/feature_metadata_caching/manifest_1.20160714105301.csv', S3Helper.generate_remote_path('manifests/manifest_1.20160714105301.csv', REMOTE_TESTING_FOLDER), MANIFEST_OPTIONS)
      S3Helper.upload_file('spec/data/files/feature_metadata_caching/feed3', S3Helper.generate_remote_path('feed', REMOTE_TESTING_FOLDER))
      @downloader.execute(CONNECTION_PARAMS)
      S3Helper.download_files(S3Helper.generate_remote_path('cache/cache_latest_metadata.json', REMOTE_TESTING_FOLDER), 'tmp/')
      expected_cache = expected_cache_object('Event-1.1')
      actual_cache = JSON.parse(File.read('tmp/cache_latest_metadata.json'))
      expect(actual_cache).to have_key('Event-1.1')
      actual_cache.delete('update_at')
      actual_cache['Event-1.1'].delete('timestamp')

      expect(actual_cache).to eq expected_cache
    end
  end

  def expected_cache_object(key)
    {
        key => {
            'year' => Date.today.year.to_s,
            'month' => Date.today.month.to_s.rjust(2, '0'),
            'day' => Date.today.day.to_s.rjust(2, '0')
        }
    }
  end

  def cleanup
    FileUtils.rm_rf('tmp')
    FileUtils.rm_rf('metadata')
    FileUtils.rm_rf('source')
    GoodData::Connectors::DownloaderCsv::S3Helper.clear_bucket(REMOTE_TESTING_FOLDER)
  end

  def execute(downloader, metadata)
    downloader.connect
    downloader.prepare_manifests

    pos = 0
    downloader.number_of_manifest_in_one_run.times do |i|
      $log.info "Starting processing number #{i + 1}"
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
end