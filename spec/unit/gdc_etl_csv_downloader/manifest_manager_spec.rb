require 'gdc_etl_csv_downloader'
require 'gooddata_connectors_backends'

describe GoodData::Connectors::DownloaderCsv::ManifestManager do
  before :all do
    ManifestManager = GoodData::Connectors::DownloaderCsv::ManifestManager
    ConnectionHelper = GoodData::Connectors::DownloaderCsv::ConnectionHelper
    Csv = GoodData::Connectors::DownloaderCsv::Csv
    Batch = GoodData::Connectors::Metadata::Batch
    Metadata = GoodData::Connectors::Metadata::Metadata
    TYPE = 'csv'.freeze
    ENTITY_NAME = 'Event'.freeze
    REMOTE_FOLDER = 'ETL_tests/WalkMe/data'.freeze

    @metadata = Metadata.new(ConnectionHelper::PARAMS)
    @downloader = Csv.new(@metadata, ConnectionHelper::PARAMS)
    @metadata.set_source_context(ConnectionHelper::DEFAULT_DOWNLOADER, {}, @downloader)
  end

  let(:obj) do
    backend = Object.new
    generate_manifests = false
    options = {
      backend: backend,
      metadata: @metadata,
      type: TYPE,
      generate_manifests: generate_manifests
    }
    ManifestManager.new(options)
  end

  it 'Should be defined' do
    expect(ManifestManager).to be_kind_of(Class)
  end

  describe 'new' do
    it 'Should raise ArgumentError for wrong options' do
      expect { ManifestManager.new(nil) }.to raise_error(ArgumentError)
    end

    it 'Should initialize' do
      expect(obj).to be_instance_of(ManifestManager)
    end
  end

  describe '#process_manifests' do
    it 'Loads and process manifests' do
      manifest_pattern = 'manifest_{time(%s)}.csv'
      parsing_info = { time_pattern: '%s', pattern: 'manifest_(?<time>\\d{10})\\.csv' }
      remote_files = %w(file1 file2)
      expected_manifests = ['manifestA']
      remote_folder = 'ETL_tests/WalkMe/data/'
      allow(obj).to receive(:process_manifest_pattern).with(manifest_pattern).and_return(parsing_info)
      allow(obj.backend).to receive(:list_unprocessed).with(remote_folder, :fast).and_return(remote_files)
      allow(obj).to receive(:load_manifest_from_remote_location)
        .with(remote_files, parsing_info).and_return(expected_manifests)
      expect(obj.process_manifests).to eq expected_manifests
      expect(obj.manifests).to eq expected_manifests
    end
    it 'Loads and process manifests with generate_manifest is true' do
      obj.generate_manifests = true
      manifest_pattern = 'manifest_{time(%s)}.csv'
      parsing_info = { time_pattern: '%s', pattern: 'manifest_(?<time>\\d{10})\\.csv' }
      remote_files = []
      expected_manifests = ['manifestA']
      allow(obj).to receive(:process_manifest_pattern).with(manifest_pattern).and_return(parsing_info)
      allow(obj).to receive(:load_manifest_from_remote_location)
                        .with(remote_files, parsing_info).and_return(expected_manifests)
      expect(obj.process_manifests).to eq expected_manifests
      expect(obj.manifests).to eq expected_manifests

    end
  end

  describe '#load_last_unprocessed_manifest' do
    before(:each) do
      @manifest = {
        'sequence' => 2
      }
      @manifest_rows = [
        { 'feed' => 'feed' },
        { 'feed' => 'feed2' }
      ]
      @options = {
        manifest_process_type: @downloader.manifest_process_type,
        previous_batch: @downloader.previous_batch,
        batch: @downloader.batch,
        all_batches: @downloader.all_batches,
        local_path: @downloader.local_path,
        encryptions: {}

      }
    end

    it 'Returns nil if it can not find manifest' do
      allow(obj).to receive(:find_manifest_to_process).and_return(nil)
      expect(obj.load_last_unprocessed_manifest(@options)).to eq nil
    end

    it 'Checks sequence' do
      allow(obj).to receive(:find_manifest_to_process).and_return(@manifest)
      obj.sequence_mode = true
      allow(obj).to receive(:check_manifest_sequence).with(@options[:previous_batch]).and_raise(Exception)
      expect { obj.load_last_unprocessed_manifest(@options) }.to raise_error(Exception)
    end

    it 'Checks manifest files and configuration file' do
      obj.generate_manifests = false
      allow(obj).to receive(:find_manifest_to_process).and_return(@manifest)
      allow(obj).to receive(:load_manifest_rows).with(@options[:local_path], @options[:batch]).and_return(@manifest_rows)
      allow(obj).to receive(:check_manifest_and_config).and_raise(Exception)
      expect { obj.load_last_unprocessed_manifest(@options) }.to raise_error(Exception)
    end

    it 'Loads manifest data' do
      manifests_data = { 'something' => 'something else' }
      obj.generate_manifests = true
      allow(obj).to receive(:find_manifest_to_process).and_return(@manifest)
      allow(obj).to receive(:validate_manifest).and_return(true)
      allow(obj).to receive(:load_manifest_rows).with(@options).and_return(@manifest_rows)
      allow(obj).to receive(:validate_manifest).with(@manifest_rows)
      allow(obj).to receive(:create_manifest_structures).with(@manifest_rows).and_return(manifests_data)
      expect(obj.load_last_unprocessed_manifest(@options)).to eq manifests_data
    end
  end

  describe '#create_dummy_manifests' do
    it 'Raises ArgumentError for wrong args' do
      expect { obj.create_dummy_manifests(nil, Object.new, @downloader) }.to raise_error(ArgumentError)
    end

    it 'Creates local manifest and sets local manifest to it' do
      hash = {
        'key' => 'event_1_1471421346.csv',
        'feed' => 'Event',
        'file_name' => 'event_1_1471421346.csv',
        'feed_version' => '1',
        'file_url' => 'some/path/event_1_1471421346.csv',
        'file_header' => 'ID,title',
        'file_column_count' => 2,
        'target_ads' => '',
        'export_type' => 'inc',
        'md5' => 'unknown',
        'timestamp' => '1471421346',
        'num_rows' => '1'
      }
      remote_obj = GoodData::Connectors::Backends::RemoteObject.new('some/path/event_1_1471421346.csv')
      remote_files = [remote_obj]
      data_files = [hash]

      remote_folder = 'ETL_tests/WalkMe/data'
      local_path = 'tmp/'
      pattern = '^manifest_(?<time>\\d{10})\\.csv'
      allow(obj.backend).to receive(:delete_files_in_folder_matching_pattern)
        .with(remote_folder, pattern)
      obj.local_manifest = []

      obj.create_dummy_manifests(local_path, remote_files, @downloader)
      expect(obj.local_manifest.last['path'].match('manifest')).to be_truthy
      path_to_new_file = Dir['tmp/manifest-*'].first
      files_identical = FileUtils.compare_file(path_to_new_file, 'spec/data/files/data_folder/referential_manifest.csv')
      expect(files_identical).to be_truthy
    end
  end

  describe '#check_manifest_sequence' do
    before(:each) do
      obj.manifest = {
        'sequence' => 3
      }
      @previous_batch = Object.new
      allow(@previous_batch).to receive(:sequence).and_return(2)
    end

    it 'Raises RuntimeError if manifest is out of sequence - with previous sequence' do
      allow(@previous_batch).to receive(:sequence).and_return(4)
      expect { obj.check_manifest_sequence(@previous_batch) }.to raise_error(RuntimeError)
    end

    it 'Raises RuntimeError if manifest is out of sequence - without previous sequence' do
      expect { obj.check_manifest_sequence(nil) }.to raise_error(RuntimeError)
    end

    it 'Does not raise anything' do
      expect { obj.check_manifest_sequence(@previous_batch) }.not_to raise_error
    end
  end

  describe '#load_manifest_rows' do
    it 'Returns array of manifest rows' do
      local_path = 'spec/data/files/data_folder/'
      batch = Batch.new('id')
      manifest_file = 'manifest_1438758475.csv'
      obj.manifest = {
        'path' => "some/path/#{manifest_file}",
        'sequence' => 2
      }
      options = {
          batch: batch,
          local_path: local_path,
          encryptions: {}
      }
      expected_output_feed_names = %w(Event Event Event User)
      allow(obj.backend).to receive(:read).with(obj.manifest['path'], local_path + manifest_file)
      expect(obj.backend).to receive(:read)
      output = obj.load_manifest_rows(options)
      output_feed_names = output.map { |row| row['feed'] }
      expect(output_feed_names).to eq expected_output_feed_names
      expect(batch.sequence).to eq obj.manifest['sequence']
      expect(batch.filename).to eq manifest_file
    end
  end

  describe '#check_manifest_and_config' do
    before(:each) do
      @downloader_entities_ids = %w(id1 id2 id3)
      obj.manifest = {}
      @manifest_rows = ['row']
      allow(obj.metadata).to receive(:get_downloader_entities_ids).and_return(@downloader_entities_ids)
    end
    it 'Raises Exception if manifest file has more entities than config file' do
      manifest_ids = %w(id1 id2)
      expect { obj.check_manifest_and_config(@manifest_rows, manifest_ids) }.to raise_error(Exception)
    end

    it 'Raises Exception if config file has more entities than manifest file' do
      manifest_ids = %w(id1 id2 id3 id4)
      expect { obj.check_manifest_and_config(@manifest_rows, manifest_ids) }.to raise_error(Exception)
    end

    it 'Raises Exception if it there missing files' do
      allow(obj).to receive(:check_manifest_files).with(@manifest_rows).and_raise(Exception)
      expect { obj.check_manifest_and_config(@manifest_rows, @downloader_entities_ids) }.to raise_error(Exception)
    end

    it 'Does not raise anything' do
      allow(obj).to receive(:check_manifest_files).with(@manifest_rows)
      expect(obj).to receive(:check_manifest_files)
      expect { obj.check_manifest_and_config(@manifest_rows, @downloader_entities_ids) }.not_to raise_error(Exception)

      allow(obj.metadata).to receive(:get_configuration_by_type_and_key).and_call_original
      manifest_ids = %w(id1 id2)
      expect { obj.check_manifest_and_config(@manifest_rows, manifest_ids) }.not_to raise_error(Exception)
    end
  end

  describe '#check_manifest_files' do
    before(:each) do
      obj.manifest = {
        'path' => 'manifest/path'
      }
      @manifest_rows = [
        { 'file_url' => 's3://some/path' },
        { 'file_url' => 'some/other/path' }
      ]
      allow(obj.backend).to receive(:parse_filename).with('s3://some/path').and_return('some/path')
    end

    it 'Raises Exception if manifest files are missing' do
      allow(obj.backend).to receive(:list).with('some/path', :old).and_return([])
      allow(obj.backend).to receive(:list).with('some/other/path', :old).and_return(['something'])
      expect { obj.check_manifest_files(@manifest_rows) }.to raise_error(Exception)
    end

    it 'Does not raise anything' do
      allow(obj.backend).to receive(:list).with('some/path', :old).and_return(['something'])
      allow(obj.backend).to receive(:list).with('some/other/path', :old).and_return(['something'])
      expect { obj.check_manifest_files(@manifest_rows) }.not_to raise_error
    end
  end

  describe '#find_manifest_to_process' do
    before(:each) do
      manifests = [
        { 'path' => 'some_path//file.json', 'sequence' => 2, 'date' => Time.at(1_471_421_346) },
        { 'path' => 'some_other_path/file2.json', 'sequence' => 1, 'date' => Time.at(1_471_421_346) },
        { 'path' => 'path_3/file3.json', 'sequence' => 1, 'date' => Time.at(1_471_421_350) }
      ]
      obj.manifests = manifests
      batch = Batch.new('id', 'file2.json')
      @all_batches = [batch]
    end

    it 'Finds in move mode' do
      manifest_process_type = 'move'
      expect(obj.find_manifest_to_process(manifest_process_type, @all_batches, 0)).to eq obj.manifests.first
    end

    it 'Finds in history mode with sequence' do
      manifest_process_type = 'history'
      obj.sequence_mode = true
      expect(obj.find_manifest_to_process(manifest_process_type, @all_batches, 0)).to eq obj.manifests[2]
    end

    it 'Finds in history mode without sequence' do
      manifest_process_type = 'history'
      expect(obj.find_manifest_to_process(manifest_process_type, @all_batches, 0)).to eq obj.manifests.first
    end
  end

  describe '#process_manifest_pattern' do
    it 'Raises exception if there is no timestamp in pattern' do
      manifest_pattern = 'manifest-{sequence}'
      expect { obj.process_manifest_pattern(manifest_pattern) }.to raise_error(Exception)
    end

    it 'Finds right pattern without sequence' do
      expected_pattern_without_sequence = { time_pattern: '%s', pattern: '^manifest_(?<time>\\d{10})\\.csv' }
      manifest_pattern = 'manifest_{time(%s)}.csv'
      expect(obj.process_manifest_pattern(manifest_pattern)).to eq expected_pattern_without_sequence
    end

    it 'Finds right pattern with sequence' do
      expected_pattern_with_sequence = { sequence_mode: true, time_pattern: '%Y%m%d%H%M%S', pattern: '^manifest-blabla_(?<sequence>\\d*)\\.(?<time>\\d{4}\\d{1,2}\\d{1,2}\\d{1,2}\\d{1,2}\\d{1,2})' }
      manifest_pattern = 'manifest-blabla_{sequence}.{time(%Y%m%d%H%M%S)}'
      expect(obj.process_manifest_pattern(manifest_pattern)).to eq expected_pattern_with_sequence

      expected_pattern_with_sequence = { time_pattern: '%Y%m%d%H%M%S', sequence_mode: true, pattern: '^manifest-blabla_(?<time>\\d{4}\\d{1,2}\\d{1,2}\\d{1,2}\\d{1,2}\\d{1,2})\\.(?<sequence>\\d*)' }
      manifest_pattern = 'manifest-blabla_{time(%Y%m%d%H%M%S)}.{sequence}'
      expect(obj.process_manifest_pattern(manifest_pattern)).to eq expected_pattern_with_sequence

      expected_pattern_with_sequence = { time_pattern: '%Y%m%d%H%M%S', sequence_mode: true, pattern: '^manifest-blalbla-(?<time>\\d{4}\\d{1,2}\\d{1,2}\\d{1,2}\\d{1,2}\\d{1,2})-(?<sequence>\\d*)' }
      manifest_pattern = 'manifest-blalbla-{time(%Y%m%d%H%M%S)}-{sequence}'
      expect(obj.process_manifest_pattern(manifest_pattern)).to eq expected_pattern_with_sequence
    end
  end

  describe '#load_manifest_from_remote_location' do
    before(:each) do
      @manifest1 = { 'sequence' => 3, 'date' => Time.at(1_471_421_346) }
      @manifest2 = { 'sequence' => 2, 'date' => Time.at(1_471_421_349) }
      @manifest3 = { 'sequence' => 1, 'date' => Time.at(1_471_421_354) }

      obj.local_manifest = [@manifest3]
      @parsing_info = {
        pattern: "manifest-batchA_(?<sequence>\d*)\.(?<time>\d{4}\d{1,2}\d{1,2}\d{1,2}\d{1,2}\d{1,2})"
      }

      allow(obj).to receive(:add_manifest_hash) do |object, _parsing_info|
        object
      end

      @remote_files = [@manifest1, @manifest2]
    end

    it 'Loads and sorts manifests with sequence' do
      obj.sequence_mode = true
      output = obj.load_manifest_from_remote_location(@remote_files, @parsing_info)
      expect(output).to eq [@manifest3, @manifest2, @manifest1]
    end

    it 'Loads and sorts manifests without sequence' do
      obj.sequence_mode = false
      output = obj.load_manifest_from_remote_location(@remote_files, @parsing_info)
      expect(output).to eq [@manifest1, @manifest2, @manifest3]
    end

    it 'Loads and sorts manifests without remote files' do
      @remote_files = []
      obj.sequence_mode = false
      output = obj.load_manifest_from_remote_location(@remote_files, @parsing_info)
      expect(output).to eq [@manifest3]
    end
  end

  describe '#add_manifest_hash' do
    before(:each) do
      @parsing_info = {
        sequence_mode: true,
        time_pattern: '%Y%m%d%H%M%S',
        pattern: 'manifest-batchA_(?<sequence>\\d*)\\.(?<time>\\d{4}\\d{1,2}\\d{1,2}\\d{1,2}\\d{1,2}\\d{1,2})'
      }
      @object = Object.new
    end

    it 'Returns nil if key is processed' do
      allow(@object).to receive(:key).and_return('processed/manifest-batchA_10.20160714105300.csv')
      expect(obj.add_manifest_hash(@object, @parsing_info)).to eq nil
    end

    it 'Returns nil if key is not matching regex' do
      allow(@object).to receive(:key).and_return('path/manifest_10.20160714105300.csv')
      expect(obj.add_manifest_hash(@object, @parsing_info)).to eq nil
    end

    it 'Returns hash' do
      expected_output = {
        'path' => 'path/manifest-batchA_10.20160714105300.csv',
        'sequence' => 10,
        'date' => Time.new(2016, 0o7, 14, 10, 53, 0),
        'regex' => nil
      }
      allow(@object).to receive(:key).and_return('path/manifest-batchA_10.20160714105300.csv')
      expect(obj.add_manifest_hash(@object, @parsing_info)).to eq expected_output
    end
  end

  describe '#create_manifest_structures' do
    it 'Raises exception if manifest file contains more versions of entities' do
      path = 'spec/data/files/data_folder/multiple_entity_versions_manifest_1438758475.csv'
      manifest_rows = []
      obj.manifest = { 'sequence' => '1', 'regex' => 'manifest' }
      CSV.open(path, headers: true, col_sep: '|').each { |line| manifest_rows << line }
      expect { obj.create_manifest_structures(manifest_rows) }.to raise_error(Exception)
    end

    it 'Creates hash of manifest data from csv rows of manifest' do
      path = 'spec/data/files/data_folder/manifest_1438758475.csv'
      manifest_rows = []
      obj.manifest = { 'sequence' => '1', 'regex' => 'manifest' }
      CSV.open(path, headers: true, col_sep: '|').each { |line| manifest_rows << line }
      output = obj.create_manifest_structures(manifest_rows)
      expected_output = {
        path: 's3://gdc-ms-connectors/ETL_tests/WalkMe/data/2015_08_16/ip-10-0-0-113_3440_1439683329951.csv',
        timestamp: '1438625626000',
        version: '1.2',
        number_of_rows: nil,
        hash: 'unknown',
        entity_regex: nil,
        export_type: 'inc',
        sequence: '1',
        sheet_path: nil,
        target_predicate: nil,
        regex: 'manifest',
        row: manifest_rows[0]
      }

      expect(output['Event'].first).to eq expected_output
    end
  end
end
