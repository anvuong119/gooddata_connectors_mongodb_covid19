require 'aws-sdk-v1'
require 'gdc_etl_csv_downloader'
require 'gooddata_connectors_backends'

describe GoodData::Connectors::DownloaderCsv::FeedManager do
  before :all do
    FeedManager = GoodData::Connectors::DownloaderCsv::FeedManager
    ManifestManager = GoodData::Connectors::DownloaderCsv::ManifestManager
    ConnectionHelper = GoodData::Connectors::DownloaderCsv::ConnectionHelper
    Csv = GoodData::Connectors::DownloaderCsv::Csv
    Metadata = GoodData::Connectors::Metadata::Metadata
    Entity = GoodData::Connectors::Metadata::Entity
    TYPE = 'csv'.freeze
    ENTITY_NAME = 'Event'.freeze
    REMOTE_FOLDER = 'ETL_tests/WalkMe/data'.freeze

    @metadata = Metadata.new(ConnectionHelper::PARAMS)
    @downloader = Csv.new(@metadata, ConnectionHelper::PARAMS)
    @metadata.set_source_context(ConnectionHelper::DEFAULT_DOWNLOADER, {}, @downloader)
    backend = Object.new
    @options = {
      backend: backend,
      metadata: @metadata,
      type: TYPE,
      generate_manifests: false
    }
    @manifest_manager = ManifestManager.new(@options)
  end

  let(:obj) do
    FeedManager.new(@options)
  end

  it 'Should be defined' do
    expect(FeedManager).to be_kind_of(Class)
  end

  describe 'new' do
    it 'Should raise ArgumentError for wrong options' do
      expect { FeedManager.new(nil) }.to raise_error(ArgumentError)
    end

    it 'Should initialize' do
      expect(obj).to be_instance_of(FeedManager)
    end
  end

  describe '#load_data_structure_file' do
    before(:each) do
      @feed_tree = {
        'key' => {
          'inner_key' => [
            { 'field_order' => '3' },
            { 'field_order' => '1' },
            { 'field_order' => '2' }
          ]
        }
      }
      @expected_feed_tree = {
        'key' => {
          'inner_key' => [
            { 'field_order' => '1' },
            { 'field_order' => '2' },
            { 'field_order' => '3' }
          ]
        }
      }
      @local_path = '/source'
      @feed_path = 'feed_path'
      @data_files = [
          {
              :entity_name => 'Country',
              :remote_filename =>
                  'some/path/csv_source/countries.csv',
              :file => {
                  :feed_version => 'default',

                  :export_type => nil,
                  :md5 => 'unknown',
                  :timestamp => 1_471_609_110,
                  :entity_regex => 'regex'
              }
          }
      ]
      remote_obj = GoodData::Connectors::Backends::RemoteObject.new('some/path1')
      @remote_files = [remote_obj]
      @hash = {
          'key'=>"countries.csv",
          'feed'=>"Country",
          'file_name'=>"countries.csv",
          'feed_version'=>nil,
          'file_url'=>"some/path/csv_source/countries.csv",
          'file_header'=>"this,is,header",
          'file_column_count'=>3,
          'export_type'=>nil,
          'entity_regex'=>"regex",
          'md5'=>nil,
          'timestamp'=>1471609110
      }
    end

    it 'Loads data without feed' do
      expect(@metadata).to receive(:get_configuration_by_type_and_key).with(TYPE, 'options|data_structure_info', String)
        .and_return(nil)
      expect(obj.backend).to receive(:read).with(@data_files.first[:remote_filename],@local_path + 'countries.csv')
      expect(obj).to receive(:read_header).with(@local_path + 'countries.csv').and_return('this,is,header')
      expect(obj).to receive(:create_columns_info).with([@hash]).and_return(@feed_tree)
      expect(obj.load_data_structure_file(@local_path,@data_files)).to eq @expected_feed_tree
    end

    it 'Loads data with feed' do
      expect(@metadata).to receive(:get_configuration_by_type_and_key).with(TYPE, 'options|data_structure_info', String)
        .and_return(@feed_path)
      expect(obj).to receive(:parse_feed_file).with(@local_path, @feed_path, {}).and_return(@feed_tree)
      expect(obj.load_data_structure_file(@local_path,@data_files)).to eq @expected_feed_tree
    end
  end
  #
  # describe '#download_remote_files' do
  #   before(:each) do
  #     @remote_obj = Object.new
  #     @remote_obj2 = Object.new
  #     @headers = 'ID,title'
  #
  #     allow(@remote_obj2).to receive(:key).and_return('bad/path')
  #   end
  #
  #   it 'Downloads uncompressed files' do
  #     path = 'some/path/event_1.0_1471421346.csv'
  #
  #     allow(obj.backend).to receive(:read) do |remote_path, local_file_path|
  #       expect(remote_path).to eq path
  #       FileUtils.mkdir_p('source') unless File.directory?('source')
  #       File.open(local_file_path, 'w') { |file| file.write(@headers + "\n") }
  #     end
  #
  #     allow(@remote_obj).to receive(:key).and_return(path)
  #
  #     remote_files = [@remote_obj, @remote_obj2]
  #
  #     hash = {
  #       'key' => 'event_1.0_1471421346.csv',
  #       'feed' => 'Event',
  #       'file_name' => 'event_1.0_1471421346.csv',
  #       'feed_version' => '1.0',
  #       'file_url' => path,
  #       'file_header' => @headers,
  #       'file_column_count' => 2,
  #       'target_ads' => '',
  #       'export_type' => nil,
  #       'entity_regex' => nil,
  #       'md5' => 'unknown',
  #       'timestamp' => '1471421346',
  #       'num_rows' => 1,
  #       'found_timestamp' => true
  #     }
  #     expected_output = [hash]
  #     expect(obj.download_remote_files(remote_files)).to eq expected_output
  #
  #     FileUtils.rm_rf('source') if File.directory?('source')
  #   end
  #
  #   it 'Downloads compressed files' do
  #     path = 'some/path/event_1_1471421346.zip'
  #     gz_path = 'some/path/event_1_1471421346.gz'
  #
  #     allow(obj.backend).to receive(:read) do |remote_path, local_file_path|
  #       expect(remote_path).to eq path
  #       allow(@remote_obj).to receive(:key).and_return(gz_path)
  #
  #       FileUtils.mkdir_p('source') unless File.directory?('source')
  #       Zip::File.open(local_file_path, Zip::File::CREATE) do |zipfile|
  #         zipfile.get_output_stream('event_1_1471421346.csv') { |f| f.puts @headers }
  #       end
  #     end
  #
  #     allow(@remote_obj).to receive(:key).and_return(path)
  #
  #     remote_files = [@remote_obj, @remote_obj2]
  #
  #     hash = {
  #       'key' => 'event_1_1471421346.gz',
  #       'feed' => 'Event',
  #       'file_name' => 'event_1_1471421346.gz',
  #       'feed_version' => '1',
  #       'file_url' => gz_path,
  #       'file_header' => @headers,
  #       'file_column_count' => 2,
  #       'target_ads' => '',
  #       'export_type' => nil,
  #       'entity_regex' => nil,
  #       'md5' => 'unknown',
  #       'timestamp' => '1471421346',
  #       'num_rows' => 1,
  #       'found_timestamp' => true
  #     }
  #     expected_output = [hash]
  #     expect(obj.download_remote_files(remote_files)).to eq expected_output
  #     FileUtils.rm_rf('source') if File.directory?('source')
  #   end
  # end

  describe '#load_temporary_fields' do
    it 'Prepares array of fields' do
      hash = {
        'id' => ENTITY_NAME,
        'name' => ENTITY_NAME,
        'version' => 'default',
        'type' => 'type'
      }
      entity = Entity.new('hash' => hash)
      field = { 'field_name' => '#field', 'field_line_object' => { 'key1' => 'key1', 'key2' => 'key2', 'key3' => nil } }
      feed_tree = {
        entity.id => {
          'default' => [
            field
          ]
        }
      }
      expected_name = '_field'
      expected_custom = { 'key1' => 'key1', 'key2' => 'key2' }
      allow(obj).to receive(:get_field_type).with(field, expected_name, entity).and_return('string-255')
      output = obj.load_temporary_fields(entity, feed_tree, 'default')
      expect(output.first.name).to eq expected_name
      expect(output.first.custom).to eq expected_custom
    end
  end

  describe '#get_field_type' do
    before(:each) do
      hash = {
        'id' => ENTITY_NAME,
        'name' => ENTITY_NAME,
        'version' => 'default',
        'type' => 'type'
      }
      @entity = Entity.new('hash' => hash)
      @field_name = 'date'
    end

    it 'Gets string' do
      field = { 'field_type' => 'string' }
      expect(obj.get_field_type(field, @field_name, @entity)).to eq 'string-255'
      field = { 'field_type' => 'string-233' }
      expect(obj.get_field_type(field, @field_name, @entity)).to eq 'string-233'
      field = { 'field_type' => 'varchar' }
      expect(obj.get_field_type(field, @field_name, @entity)).to eq 'string-255'
    end

    it 'Gets integer' do
      field = { 'field_type' => 'integer' }
      expect(obj.get_field_type(field, @field_name, @entity)).to eq 'integer'
      field = { 'field_type' => 'bigint' }
      expect(obj.get_field_type(field, @field_name, @entity)).to eq 'integer'
    end

    it 'Gets float' do
      field = { 'field_type' => 'numeric' }
      expect(obj.get_field_type(field, @field_name, @entity)).to eq 'decimal-16-4'
      field = { 'field_type' => 'decimal-16-3' }
      expect(obj.get_field_type(field, @field_name, @entity)).to eq 'decimal-16-3'
    end

    it 'Gets boolean' do
      field = { 'field_type' => 'boolean' }
      expect(obj.get_field_type(field, @field_name, @entity)).to eq 'boolean'
    end

    it 'Gets date' do
      field = { 'field_type' => 'date' }
      expect(obj.get_field_type(field, @field_name, @entity)).to eq 'date-false'
      field = { 'field_type' => 'time-false' }
      expect(obj.get_field_type(field, @field_name, @entity)).to eq 'date-false'
    end

    it 'Gets datetime' do
      field = { 'field_type' => 'datetime' }
      expect(obj.get_field_type(field, @field_name, @entity)).to eq 'date-true'
      field = { 'field_type' => 'time-true' }
      expect(obj.get_field_type(field, @field_name, @entity)).to eq 'date-true'
      field = { 'field_type' => 'timestamp' }
      expect(obj.get_field_type(field, @field_name, @entity)).to eq 'timestamp'
    end

    it 'Gets time' do
      field = { 'field_type' => 'time' }
      expect(obj.get_field_type(field, @field_name, @entity)).to eq 'time'
    end

    it 'Defaults to string' do
      field = { 'field_type' => 'some_mysterious_field' }
      expect { obj.get_field_type(field, @field_name, @entity) }.to raise_error RuntimeError
    end
  end

  describe '#create_columns_info' do
    it 'Generates feed - creates columns' do
      data_files = [
        {
          'key' => 'countries.csv',
          'feed' => 'Country',
          'file_name' => 'countries.csv',
          'feed_version' => 'default',
          'file_url' =>
           'some/path/csv_source/countries.csv',
          'file_header' => "ID,Country\n",
          'file_column_count' => 2,
          'target_ads' => '',
          'export_type' => nil,
          'md5' => 'unknown',
          'timestamp' => 1_471_609_110,
          'num_rows' => '2'
        }
      ]
      expected_output = {
        'Country' => {
          'default' => [
            {
              'field_name' => 'ID',
              'field_type' => 'string-255',
              'field_order' => '0',
              'field_line_object' =>
              {
                'file' => 'countries.csv',
                'version' => 'default',
                'field' => 'ID',
                'type' => 'string-255',
                'order' => '0'
              }
            },
            {
              'field_name' => "Country\n",
              'field_type' => 'string-255',
              'field_order' => '1',
              'field_line_object' => {
                'file' => 'countries.csv',
                'version' => 'default',
                'field' => "Country\n",
                'type' => 'string-255',
                'order' => '1'
              }
            }
          ]
        }
      }
      expect(obj.create_columns_info(data_files)).to eq expected_output
    end
  end

  describe '#parse_feed_file' do
    it 'Parses feed file' do
      data_structure_path = 'ETL_tests/WalkMe/data/feed.csv'
      local_path = 'spec/data/files/data_folder/'
      path_with_feed_file = local_path + 'feed.csv'
      allow(obj.backend).to receive(:read)
      expect(obj.backend).to receive(:read).with(data_structure_path, path_with_feed_file)

      output = obj.parse_feed_file(local_path, data_structure_path, {})
      expect(output['Event']['1.2'][38]['field_name']).to eq 'data.main'
      expect(output['Event']['1.1'][30]['field_order']).to eq '30'
      expect(output['Event']['1.1'][30]['field_type']).to eq 'string-255'
    end
  end

  describe '#read_header' do
    before(:each) do
      @expected_output = 'Lorem ipsum'
    end
    it 'Reads compressed file header' do
      data_path = 'spec/data/files/data_folder/dummy/file2.gz'
      expect(obj.send(:read_header, data_path)).to eq @expected_output
    end

    it 'Reads file header' do
      data_path = 'spec/data/files/data_folder/dummy/feed.txt'
      expect(obj.send(:read_header, data_path)).to eq @expected_output
    end
  end

  describe '#row_count' do
    before(:each) do
      @expected_output = 1
    end

    it 'Reads compressed file row count' do
      data_path = 'spec/data/files/data_folder/dummy/file2.gz'
      expect(obj.send(:row_count, data_path)).to eq @expected_output
    end

    it 'Reads file row count' do
      data_path = 'spec/data/files/data_folder/dummy/feed.txt'
      expect(obj.send(:row_count, data_path)).to eq @expected_output
    end
  end
end
