require 'aws-sdk-v1'
require 'gdc_etl_csv_downloader'

describe GoodData::Connectors::DownloaderCsv::Csv do
  before :all do
    Csv = GoodData::Connectors::DownloaderCsv::Csv
    ManifestManager = GoodData::Connectors::DownloaderCsv::ManifestManager
    Batch = GoodData::Connectors::Metadata::Batch
    Metadata = GoodData::Connectors::Metadata::Metadata
    Entity = GoodData::Connectors::Metadata::Entity
    Field = GoodData::Connectors::Metadata::Field
    LinkFile = GoodData::Connectors::Metadata::LinkFile
    TYPE = 'csv'.freeze
    BATCH_ID = GoodData::Connectors::DownloaderCsv::ConnectionHelper::DEFAULT_DOWNLOADER
    ENTITY_NAME = 'Event'.freeze
    REMOTE_FOLDER = 'ETL_tests/WalkMe/data'.freeze
	  REMOTE_TESTING_FOLDER = GoodData::Connectors::DownloaderCsv::ConnectionHelper::DEFAULT_TIMESTAMP
  end

  let(:metadata) do
    GoodData::Connectors::Metadata::Metadata.new(GoodData::Connectors::DownloaderCsv::ConnectionHelper::PARAMS)
  end

  let(:downloader) do
    downloader = GoodData::Connectors::DownloaderCsv::Csv.new(metadata, GoodData::Connectors::DownloaderCsv::ConnectionHelper::PARAMS)
    metadata.set_source_context(GoodData::Connectors::DownloaderCsv::ConnectionHelper::DEFAULT_DOWNLOADER, {}, downloader)
    downloader.backend = Object.new
    options = {
      backend: downloader.backend,
      metadata: downloader.metadata,
      type: TYPE,
      generate_manifests: false
    }
    downloader.manifest_manager = ManifestManager.new(options)
    downloader
  end

  let(:entity) do
    hash = {
      'id' => ENTITY_NAME,
      'name' => ENTITY_NAME,
      'version' => 'default',
      'type' => 'type'
    }
    Entity.new('hash' => hash)
  end

  it 'Should be defined' do
    expect(Csv).to be_kind_of(Class)
  end

  describe 'new' do
    it 'Should raise ArgumentError for wrong options' do
      expect { Csv.new(nil, nil) }.to raise_error(ArgumentError)
    end

    it 'Should initialize' do
      expect(downloader).to be_instance_of(Csv)
    end
  end

  describe '#connect' do
    it 'Connects' do
      expect(downloader).to receive(:connect_backend).and_call_original
      expect(downloader).to receive(:load_parameters).and_call_original
      expect(downloader).to receive(:load_batches).and_call_original
      downloader.connect
    end

    describe 'connects unsuccesfully' do
      it 'Raises exception if incorrect manifest_process_type_values' do
        allow(metadata).to receive(:get_configuration_by_type_and_key).and_call_original
        allow(metadata).to receive(:get_configuration_by_type_and_key)
          .with(TYPE, 'options|manifest_process_type', String, 'move')
          .and_return('something')
        expect { downloader.connect }.to raise_error(Exception)
      end

      it 'Raises exception if manifest_process_type=history and generate_manifest=true ' do
        allow(metadata).to receive(:get_configuration_by_type_and_key).and_call_original
        allow(metadata).to receive(:get_configuration_by_type_and_key)
          .with(TYPE, 'options|generate_manifests', Boolean).and_return(true)
        allow(metadata).to receive(:get_configuration_by_type_and_key)
          .with(TYPE, 'options|manifest_process_type', String, 'move')
          .and_return('history')
        expect { downloader.connect }.to raise_error(Exception)
      end
    end
  end

  describe '#connect_backend' do
    it 'Creates backend and connects it' do
      expect(downloader.connect_backend).to be_instance_of(GoodData::Connectors::Backends::S3)
    end
  end

  describe 'load_parameters' do
    it 'Loads parameters to downloader (checks readable)' do
      downloader.connect_backend
      downloader.load_parameters
      expect(downloader.number_of_manifest_in_one_run).to eq 1
      expect(downloader.manifest_process_type).to eq 'history'
    end
  end

  describe '#load_batches' do
    it 'Loads batches' do
      batches = {
        previous_batch: 'previous_batch',
        all_batches: 'all_batches'
      }
      allow(metadata).to receive(:get_configuration_by_type_and_key).with('global', 'ID').and_return(BATCH_ID)
      allow(downloader).to receive(:initialize_batches).with(nil, BATCH_ID).and_return(batches)
      expect(downloader.load_batches).to eq batches[:all_batches]
      expect(downloader.batch).to be_instance_of(Batch)
      expect(downloader.previous_batch).to eq batches[:previous_batch]
      expect(downloader.all_batches).to eq batches[:all_batches]
    end
  end

  describe '#prepare_next_load' do
    before(:each) do
      @last_batch = Batch.new('id')
      @files_to_download = []
      downloader.all_batches = []
      downloader.batch_id = BATCH_ID
    end

    it 'Prepares batch if history' do
      downloader.manifest_process_type = 'history'
      downloader.prepare_next_load(@last_batch)
      expect(downloader.all_batches).to eq [@last_batch]
      expect(downloader.previous_batch).to eq @last_batch
      expect(downloader.batch).to be_instance_of(Batch)
      expect(downloader.files_to_download).to eq @files_to_download
    end

    it 'Prepares batch if move' do
      downloader.manifest_process_type = 'move'
      downloader.prepare_next_load(@last_batch)
      expect(downloader.all_batches).to eq []
      expect(downloader.previous_batch).to eq @last_batch
      expect(downloader.batch).to be_instance_of(Batch)
      expect(downloader.files_to_download).to eq @files_to_download
    end
  end

  describe '#load_data_structure_file' do
    it 'Loads data with generate_manifests and data_structure_info' do
      expected_feed_tree = {
        'key' => {
          'inner_key' => [
            { 'field_order' => '1' },
            { 'field_order' => '2' },
            { 'field_order' => '3' }
          ]
        }
      }
      downloader.files_to_download = ['file1']
      downloader.local_path = '/source'
      allow(downloader.feed_manager).to receive(:load_data_structure_file)
        .with('/source',['file1'], {}).and_return expected_feed_tree
      expect(downloader.load_data_structure_file).to eq expected_feed_tree
      expect(downloader.feed_tree).to eq expected_feed_tree
    end
  end

  describe '#load_last_unprocessed_manifest' do
    it 'Loads manifest data' do
      options = {
        manifest_process_type: downloader.manifest_process_type,
        previous_batch: downloader.previous_batch,
        batch: downloader.batch,
        all_batches: downloader.all_batches,
        local_path: downloader.local_path,
        position: 0,
        encryptions: {}
      }
      manifests_data = { 'something' => 'something else' }
      allow(downloader.manifest_manager).to receive(:load_last_unprocessed_manifest)
        .with(options).and_return manifests_data
      expect(downloader.load_last_unprocessed_manifest).to eq manifests_data
    end
  end

  describe '#load_metadata' do
    before(:each) do
      @manifests_data = {
        'Event' => [
          { version: 'default' },
          { version: '2' }
        ]
      }
    end

    it 'Returns entity and does nothing if manifest file does not contain entity' do
      expect(downloader.load_metadata(ENTITY_NAME).name).to eq ENTITY_NAME
    end

    it 'Returns entity(with version) if entity is disabled' do
      downloader.manifests_data = @manifests_data
      hash = {
        'id' => ENTITY_NAME,
        'name' => ENTITY_NAME,
        'version' => 'default',
        'type' => 'type',
        'enabled' => false
      }
      disabled_entity = Entity.new('hash' => hash)
      allow(downloader).to receive(:load_metadata_entity).with(downloader.metadata, ENTITY_NAME, 'default')
        .and_return(disabled_entity)
      expect(downloader.load_metadata(ENTITY_NAME)).to eq disabled_entity
    end

    it 'Raises exception if entity is not present in feed file' do
      downloader.manifests_data = @manifests_data

      allow(downloader).to receive(:load_metadata_entity).with(downloader.metadata, ENTITY_NAME, 'default')
        .and_return(entity)
      expect { downloader.load_metadata(ENTITY_NAME) }.to raise_error(Exception)
    end

    it 'Raises exception if entity with current entity version is not present in feed file' do
      feed_tree = {
        ENTITY_NAME => {
          '5' => {}
        }
      }
      downloader.feed_tree = feed_tree
      downloader.manifests_data = @manifests_data

      allow(downloader).to receive(:load_metadata_entity).with(downloader.metadata, ENTITY_NAME, 'default')
        .and_return(entity)
      expect { downloader.load_metadata(ENTITY_NAME) }.to raise_error(Exception)
    end

    it 'Diffs fields and saves entity' do
      feed_tree = {
        ENTITY_NAME => {
          'default' => {}
        }
      }
      temporary_fields = { 'fields' => {} }
      diff = {}
      downloader.feed_tree = feed_tree
      downloader.manifests_data = @manifests_data
      hash = {
        'id' => ENTITY_NAME,
        'name' => ENTITY_NAME,
        'version' => 'default',
        'type' => 'type',
        'custom' => {}
      }
      entity = Entity.new('hash' => hash)

      allow(downloader).to receive(:load_metadata_entity).with(downloader.metadata, ENTITY_NAME, 'default')
        .and_return(entity)
      allow(downloader.feed_manager).to receive(:load_temporary_fields).with(entity, feed_tree, 'default')
        .and_return(temporary_fields)
      allow(downloader).to receive(:diff_entity_fields).with(downloader.metadata, diff, entity)
      expect(downloader).to receive(:diff_entity_fields)
      allow(downloader).to receive(:set_parsing_information).with(downloader.metadata, TYPE, entity)
        .and_return('OK')
      expect(downloader).to receive(:set_parsing_information)
      allow(entity).to receive(:diff_fields).with(temporary_fields).and_return(diff)
      allow(downloader.metadata).to receive(:save_entity).with(entity).and_return('OK')

      expect(downloader.load_metadata(ENTITY_NAME)).to eq 'OK'
      expect(entity.custom['download_by']).to eq TYPE
      expect(entity.dirty).to eq true
    end
  end

  describe '#load_metadata_entity' do
    it 'Creates new entity if it cant find it' do
      entity_name = 'User'
      expect(downloader.load_metadata_entity(downloader.metadata, entity_name, 'default').name).to eq entity_name
    end

    it 'Loads entity' do
      expect(downloader.load_metadata_entity(downloader.metadata, ENTITY_NAME, 'default').name).to eq ENTITY_NAME
    end
  end

  describe '#diff_entity_fields' do
    it 'Diffs fields' do
      new_field1_hash = {
        'id' => 'id_name',
        'name' => 'id_name',
        'order' => 'g1',
        'type' => 'integer-true',
        'custom' => {},
        'enabled' => true
      }
      new_field2_hash = new_field1_hash.dup
      new_field2_hash['id'] = 'name2'
      new_field2_hash['name'] = 'name2'
      new_field1 = Field.new('hash' => new_field1_hash)
      new_field2 = Field.new('hash' => new_field2_hash)
      old_field = Field.new('hash' => new_field1_hash.dup)
      source_field = Field.new('hash' => new_field1_hash.dup)
      target_field_hash = new_field1_hash.dup
      new_name = 'new name'
      target_field_hash['name'] = new_name
      target_field = Field.new('hash' => target_field_hash)
      change = {
        'source_field' => source_field,
        'target_field' => target_field,
        'name' => true
      }
      diff = {
        'only_in_target' => [new_field1, new_field2],
        'only_in_source' => [old_field],
        'changed' => [change]
      }
      allow(downloader.metadata).to receive(:load_fields_from_source?).with(entity.id).and_return true

      allow(downloader).to receive(:add_new_field).with(new_field1, entity)
      allow(downloader).to receive(:add_new_field).with(new_field2, entity)
      expect(downloader).to receive(:add_new_field).exactly(2).times

      allow(downloader).to receive(:disable_field).with(old_field, entity) do
        old_field.enabled = false
      end

      allow(downloader).to receive(:change_field).with(change, entity) do
        source_field.name = new_name
      end

      downloader.diff_entity_fields(metadata, diff, entity)

      expect(old_field.enabled).to eq false
      expect(source_field.name).to eq new_name
    end
  end

  describe '#add_new_field' do
    it 'Adds new field' do
      new_field_hash = {
        'id' => 'id_name',
        'name' => 'id_name',
        'order' => 'g1',
        'type' => 'integer-true',
        'custom' => {},
        'enabled' => true
      }
      new_field = Field.new('hash' => new_field_hash)
      new_order_id = 'g2'

      allow(entity).to receive(:get_new_order_id).and_return(new_order_id)
      allow(entity).to receive(:add_field).with(new_field)
      expect(entity).to receive(:add_field)
      downloader.add_new_field(new_field, entity)
      expect(entity.dirty).to eq true
      expect(new_field.order).to eq new_order_id
    end
  end

  describe '#disable_field' do
    it 'Disables field' do
      hash = {
        'id' => 'id_name',
        'name' => 'id_name',
        'order' => 'g1',
        'type' => 'integer-true',
        'custom' => {},
        'enabled' => true
      }
      field = Field.new('hash' => hash)

      downloader.disable_field(field, entity)
      expect(entity.dirty).to eq true
      expect(field.enabled).to eq false
    end
  end

  describe '#change_field' do
    before(:each) do
      source_field_hash = {
        'id' => 'id_name',
        'name' => 'id_name',
        'order' => 'g1',
        'type' => 'integer-true',
        'custom' => {},
        'enabled' => true
      }
      @target_field_hash = source_field_hash.dup
      @target_field_hash['id'] = 'name2'
      @target_field_hash['name'] = 'name2'
      @source_field = Field.new('hash' => source_field_hash)
      @target_field = Field.new('hash' => @target_field_hash)
    end

    it 'Raises exception if type has changed - not supported' do
      change = {
        'source_field' => @source_field,
        'target_field' => @target_field,
        'name' => true,
        'type' => true
      }
      expect { downloader.change_field(change, entity) }.to raise_error(Exception)
    end

    it 'Changes name of source field' do
      change = {
        'source_field' => @source_field,
        'target_field' => @target_field,
        'name' => true
      }
      downloader.change_field(change, entity)
      expect(@source_field.name).to eq @target_field_hash['name']
      expect(entity.dirty).to eq true
    end
  end

  describe '#set_parsing_information' do
    before(:all) do
      @parsing_info = { 'skip_rows' => '1', 'column_separator' => ',' }
    end

    it 'Does not make entity dirty if nothing changes' do
      entity.custom = @parsing_info
      downloader.set_parsing_information(downloader.metadata, TYPE, entity)
      expect(entity.custom).to eq @parsing_info
      expect(entity.dirty).to eq false
    end

    it 'Save parsing info to entity' do
      entity.custom = {}
      downloader.set_parsing_information(downloader.metadata, TYPE, entity)
      expect(entity.custom).to eq @parsing_info
      expect(entity.dirty).to eq true
    end
  end

  describe '#download_entity_data' do
    it 'Load files data from manifest' do
      path_without_bucket = 'ETL_tests/some/path/'
      path = "s3://gdc-ms-connectors/#{path_without_bucket}"
      manifests_data = {
        ENTITY_NAME => [
          { path: "#{path}file1" },
          { path: "#{path}file2" }
        ]
      }
      downloader.manifests_data = manifests_data
      downloader.local_path = '/some/local/path/'
      batch = Batch.new('id', '', '1')
      downloader.batch = batch
      downloader.files_to_download = []
      downloader.download_entity_data(ENTITY_NAME)
      counter = 1
      downloader.files_to_download.sort_by{|file| file[:local_filename]}.each do |file|
        expect(file[:remote_filename]).to eq "#{path_without_bucket}file" + counter.to_s
        expect(file[:local_filename]).to eq  '/some/local/path/file' + counter.to_s
        expect(file[:entity_name]).to eq ENTITY_NAME
        expect(file[:batch]).to be_instance_of(String)
        expect(file[:now]).to be_instance_of(Time)
        expect(file[:file]).to eq manifests_data[ENTITY_NAME][counter - 1]
        counter += 1
      end
    end
  end

  describe '#define_default_entities' do
    it 'Returns empty array' do
      arr = []
      expect(downloader.define_default_entities).to eq arr
    end
  end

  describe '#save_batch' do
    it 'Saves batch' do
      batch = Batch.new('id')
      downloader.batch = batch
      allow(downloader.metadata).to receive(:save_batch).with(batch).and_return('OK')
      expect(downloader.save_batch).to eq 'OK'
    end
  end

  describe '#finish_load' do
    before(:each) do
      files_to_download = [
        { entity: ENTITY_NAME, file: {}, remote_filename: 'path' },
        { entity: ENTITY_NAME, file: {}, remote_filename: 'path2' }
      ]
      downloader.files_to_download = files_to_download
      @batch = Batch.new('id')
      downloader.batch = @batch
    end

    it 'Downloads data & clears everything' do
      allow(downloader.metadata).to receive(:get_configuration_by_type_and_key)
        .with(TYPE, 'options|delete_data_after_processing', Boolean, false)
        .and_return(true)
      allow(downloader.metadata).to receive(:get_configuration_by_type_and_key)
                                        .with(TYPE, 'options|move_manifests_after_processing_to_path', String)
                                        .and_return(nil)
      allow(downloader.metadata).to receive(:get_configuration_by_type_and_key)
        .with(TYPE, 'options|move_data_after_processing_to_path', String)
        .and_return(nil)
      allow(downloader.metadata).to receive(:get_configuration_by_type_and_key)
                                        .with(TYPE, 'options|keep_data_after_processing', Boolean, false)
                                        .and_return(false)
      allow(downloader.metadata).to receive(:get_configuration_by_type_and_key)
                                        .with(TYPE, 'type', String)
                                        .and_return('S3')
      allow(downloader).to receive(:backup_settings)

      allow(downloader.backend).to receive(:delete).with('path')
      allow(downloader.backend).to receive(:delete).with('path2')
      expect(downloader.backend).to receive(:delete).exactly(2).times
      allow(downloader.backend).to receive(:exists?).with('path').and_return(true)
      allow(downloader.backend).to receive(:exists?).with('path2').and_return(true)
      expect(downloader.metadata).to receive(:save_batch).with(@batch)
      allow(downloader.backend).to receive(:disconnect)

      downloader.finish_load({})
      expect(downloader.files_to_download).to eq []
      expect(downloader.manifests_data).to eq({})
    end

    it 'Downloads data with move' do
      allow(downloader.metadata).to receive(:get_configuration_by_type_and_key)
                                        .with(TYPE, 'options|move_data_after_processing_to_path', String)
                                        .and_return(nil)
      allow(downloader.metadata).to receive(:get_configuration_by_type_and_key)
                                        .with(TYPE, 'type', String)
                                        .and_return('S3')
      allow(downloader.metadata).to receive(:get_configuration_by_type_and_key)
        .with(TYPE, 'options|delete_data_after_processing', Boolean, false)
        .and_return(false)
      allow(downloader.metadata).to receive(:get_configuration_by_type_and_key)
                                        .with(TYPE, 'options|move_manifests_after_processing_to_path', String)
                                        .and_return(nil)
      allow(downloader.metadata).to receive(:get_configuration_by_type_and_key)
                                        .with(TYPE, 'options|keep_data_after_processing', Boolean, false)
                                        .and_return(false)
      expect(downloader.metadata).to receive(:save_batch).with(@batch)
      allow(downloader).to receive(:backup_settings)
      downloader.manifest_process_type = 'move'
      downloader.manifest_manager.manifest = { 'path' => 'some/manifest/path' }
      allow(downloader.backend).to receive(:rename).with('some/manifest/path', 'some/processed/manifest/path')
      expect(downloader.backend).to receive(:rename)
      downloader.manifest_manager.manifests = [downloader.manifest_manager.manifest, { 'path' => 'some/other/path' }]
      allow(downloader.backend).to receive(:disconnect)

      downloader.finish_load({})
      expect(downloader.files_to_download).to eq []
      expect(downloader.manifests_data).to eq({})
      expect(downloader.manifest_manager.manifests).to eq [{ 'path' => 'some/other/path' }]
    end
  end

  describe '#download_data_in_parallel' do
    before(:each) do
      @config = downloader.metadata.get_configuration_by_type_and_key(TYPE, 'options', Hash)
      batch = Batch.new('id')
      downloader.batch = batch
      downloader.files_to_download = [{ entity_name: ENTITY_NAME }, { entity_name: 'Other Entity' }]
      downloader.generate_manifests = true
      allow(downloader.backend).to receive(:normalize_options).with(@config, 'number_of_threads')
        .and_return(2)
    end

    it 'Downloads data without link file' do
      allow(downloader.backend).to receive(:normalize_options).with(@config, 'ignore_check_sum')
        .and_return(false)
      downloader.use_link_file = false
      output_queue = nil
      allow(downloader).to receive(:load_thread) do |queue, _batch, use_link_file, ignore_check_sum, _semaphore|
        expect(use_link_file).to eq false
        expect(ignore_check_sum).to eq false
        Thread.new do
          queue.pop(true)
          output_queue = queue
        end
      end
      downloader.download_data_in_parallel
      expect(output_queue.empty?).to eq true
    end

    it 'Downloads data with link file' do
      allow(downloader.backend).to receive(:normalize_options).with(@config, 'ignore_check_sum')
        .and_return(false)
      downloader.use_link_file = true
      counter = 0
      output_queue = nil
      allow(downloader).to receive(:load_thread) do |queue, _batch, use_link_file, ignore_check_sum, _semaphore|
        expect(use_link_file).to eq true
        expect(ignore_check_sum).to eq false
        Thread.new do
          2.times { queue.pop(true) }
          output_queue = queue
          counter += 1
        end
      end
      downloader.download_data_in_parallel
      expect(counter).to eq 1
      expect(output_queue.empty?).to eq true
    end
  end

  describe '#load_thread' do
    before(:each) do
      @queue = Queue.new
      @file1 = {
        entity_name: ENTITY_NAME,
        file: {
          version: 1
        }
      }
      @file2 = {
        entity_name: 'Address',
        file: {
          version: 1
        }
      }
      @queue << [@file1]
      @queue << [@file2]

      batch = Batch.new('id')
      downloader.batch = batch

      @event = Entity.new('id' => ENTITY_NAME, 'name' => ENTITY_NAME)
      @address = Entity.new('id' => 'Address', 'name' => 'Address')

      @response_file1 = { path: 'path1' }
      @response_file2 = { path: 'path2' }

      @expected_batch_files = [
        { 'entity' => ENTITY_NAME, 'file' => @response_file1[:path] },
        { 'entity' => 'Address', 'file' => @response_file2[:path] }
      ]

      allow(downloader.metadata).to receive(:get_entity).with(ENTITY_NAME, 1).and_return @event
      allow(downloader.metadata).to receive(:get_entity).with('Address', 1).and_return @address

      allow(downloader).to receive(:attach_runtime_metadata).with(@event, @file1)
      allow(downloader).to receive(:attach_runtime_metadata).with(@address, @file2)
      expect(downloader).to receive(:attach_runtime_metadata).exactly(2).times
    end

    it 'Loads thread with link_file' do
      allow(downloader).to receive(:save_entity_data_with_link_file)
        .with(@event, @file1, false)
        .and_return(@response_file1)
      allow(downloader).to receive(:save_entity_data_with_link_file)
        .with(@address, @file2, false)
        .and_return(@response_file2)
      threads = []
      threads << downloader.load_thread(@queue, downloader.batch, true, false, Mutex.new)
      threads.each(&:join)
      expect(downloader.batch.files).to eq @expected_batch_files
    end

    it 'Loads thread without link_file' do
      downloader.generate_manifests = false
      allow(downloader).to receive(:save_entity_data)
        .with(@event, @file1, false)
        .and_return(@response_file1)
      allow(downloader).to receive(:save_entity_data)
        .with(@address, @file2, false)
        .and_return(@response_file2)
      threads = []
      threads << downloader.load_thread(@queue, downloader.batch, false, false, Mutex.new)
      threads.each(&:join)
      expect(downloader.batch.files).to eq @expected_batch_files
    end
  end

  describe '#save_entity_data' do
    before(:each) do
      allow(SecureRandom).to receive(:urlsafe_base64).with(6).and_return('kXt0ZbFk')
      @entity = Entity.new('id' => ENTITY_NAME, 'name' => ENTITY_NAME)
      @expected_response = { status: :ok, path: "ETL_tests/connectors_downloader_csv/#{REMOTE_TESTING_FOLDER}/csv_downloader_1/Event/2016/08/17/1471421346000000_data_kXt0ZbFk" }
    end

    it 'Saves with zipped filename && ignore_columns_check' do
      local_filename = 'spec/data/files/data_folder/dummy/file.zip'
      remote_filename = 'some/remote/data/path'
      file_hash = {
        local_filename: local_filename,
        remote_filename: remote_filename,
        now: Time.at(1_471_421_346),
        file: {}
      }
      gzip_file = 'spec/data/files/data_folder/dummy/file.gz'
      File.new(gzip_file, 'w')

      allow(File).to receive(:zip_to_gzip).and_return(gzip_file)
      expect(downloader).not_to receive(:check_data_columns)
      allow(metadata).to receive(:get_configuration_by_type_and_key)
                             .with(TYPE, 'options|ignore_columns_check', Boolean, true).and_return(true)
      allow(metadata).to receive(:get_configuration_by_type_and_key)
                             .with(TYPE, 'options|data_structure_info', String).and_return('path')
      allow(downloader.backend).to receive(:read).with(remote_filename, local_filename)
                                       .and_return(true)

      output = downloader.save_entity_data(@entity, file_hash, true)
      expected_response = @expected_response.dup
      expected_response[:path] = expected_response[:path] << '.gz'
      expect(output).to eq expected_response
      expect(@entity.runtime['global']['source_filename']).to eq gzip_file
      expect(File.exist?(gzip_file)).to be_falsey
    end

    it 'Saves with checksum' do
      local_filename = 'spec/data/files/data_folder/dummy/file'
      remote_filename = 'some/remote/data/path'
      file_hash = {
        local_filename: local_filename,
        remote_filename: remote_filename,
        now: Time.at(1_471_421_346),
        file: {
          hash: 'hash'
        }
      }
      File.new(local_filename, 'w')

      allow(downloader).to receive(:check_md5)
        .with(file_hash[:local_filename], file_hash[:file][:hash], file_hash[:remote_filename])
      expect(downloader).to receive(:check_md5)
      allow(metadata).to receive(:get_configuration_by_type_and_key)
        .with(TYPE, 'options|ignore_columns_check', Boolean, true).and_return(false)
      allow(downloader).to receive(:check_data_columns).with(@entity, file_hash)
      expect(downloader).to receive(:check_data_columns)
      downloader.generate_feed = true

      output = downloader.save_entity_data(@entity, file_hash, false)
      expect(output).to eq @expected_response
      expect(@entity.runtime['global']['source_filename']).to eq local_filename
      expect(File.exist?(local_filename)).to be_falsey
    end
  end

  describe '#save_entity_data_with_link_file' do
    before(:each) do
      @local_filename = 'spec/data/files/data_folder/dummy/file'
      @remote_filename = 'some/remote/data/path'
      @entity = Entity.new('id' => ENTITY_NAME, 'name' => ENTITY_NAME)
      @file_hash = {
        local_filename: @local_filename,
        remote_filename: @remote_filename,
        now: Time.at(1_471_421_346),
        file: {
          hash: 'hash'
        }
      }
      @link_file = LinkFile.new
      allow(downloader).to receive(:create_link_file).and_return @link_file
      @file_local_path = 'some/local/path'
      allow(@link_file).to receive(:create_file).and_return @file_local_path
      allow(downloader.metadata).to receive(:save_data).with(@entity, nil, @file_hash[:now]).and_return('OK')
    end

    it 'Saves with checksum' do
      allow(downloader).to receive(:check_md5)
        .with(@file_hash[:local_filename], @file_hash[:file][:hash], @file_hash[:remote_filename], true)
      expect(downloader).to receive(:check_md5)

      output = downloader.save_entity_data_with_link_file(@entity, @file_hash, false)
      expect(output).to eq 'OK'
      expect(@entity.runtime['global']['source_filename']).to eq @file_local_path
      expect(@entity.runtime['global']['type']).to eq 'link'
    end

    it 'Saves without checksum' do
      expect(downloader).not_to receive(:check_md5)

      output = downloader.save_entity_data_with_link_file(@entity, @file_hash, true)
      expect(output).to eq 'OK'
      expect(@entity.runtime['global']['source_filename']).to eq @file_local_path
      expect(@entity.runtime['global']['type']).to eq 'link'
    end
  end

  describe '#check_data_columns' do
    before(:each) do
      entity.add_field Field.new('id' => 'ID')
      entity.add_field Field.new('id' => 'Title')
      entity.add_field Field.new('id' => 'Year')

      @file_hash = {
        remote_filename: 'some/bds/path/events.csv',
        local_filename: '',
        entity_name: ENTITY_NAME,
        batch: '1472643751_5_batch.json',
        now: Time.at(1_468_503_659),
        file:         {
          path: 'some/bds/path/events.csv',
          timestamp: '1468503659',
          version: '1.0',
          number_of_rows: nil,
          hash: 'unknown',
          export_type: 'full',
          sequence: 5,
          regex: nil,
          row: 'CSV row'
        }
      }
    end
    it 'Raises exception for bad gzip file' do
      @file_hash[:local_filename] = 'spec/data/files/data_folder/events.gz'
      expect { downloader.check_data_columns(entity, @file_hash) }.to raise_error(Exception)
    end

    it 'Raises exception for bad csv file' do
      @file_hash[:local_filename] = 'spec/data/files/data_folder/events.csv'
      expect { downloader.check_data_columns(entity, @file_hash) }.to raise_error(Exception)
    end

    it 'Does not raise anything for OK gz file' do
      @file_hash[:local_filename] = 'spec/data/files/data_folder/events_ok.gz'
      expect { downloader.check_data_columns(entity, @file_hash) }.not_to raise_error(Exception)
    end

    it 'Does not raise anything for OK csv file' do
      @file_hash[:local_filename] = 'spec/data/files/data_folder/events_ok.csv'
      allow(entity.fields).to receive(:keys).and_return(%w(id title year))
      expect { downloader.check_data_columns(entity, @file_hash) }.not_to raise_error(Exception)
    end
  end

  describe '#check_md5' do
    before(:each) do
      @filename = 'spec/data/files/data_folder/dummy/file.zip'
      @checksum = 'd41d8cd98f00b204e9800998ecf8427e'
      @remote_filename = 'some/path/file.zip'
    end

    it 'Checks remotely' do
      remote_object = Object.new
      allow(downloader.backend).to receive(:object).with(@remote_filename).and_return(remote_object)
      allow(remote_object).to receive(:etag).and_return("'d41d8cd98f00b204e9800998ecf8427e'")
      downloader.check_md5(@filename, @checksum, @remote_filename, true)
    end

    it 'Checks local file' do
      expect { downloader.check_md5(@filename, @checksum, @remote_filename, false) }.not_to raise_error(Exception)
    end

    it 'Checks local file with checksum control off' do
      expect { downloader.check_md5(@filename, 'unknown', @remote_filename, false) }.not_to raise_error(Exception)
    end

    it 'Raises exception if checksums does not fit' do
      filename = 'spec/data/files/data_folder/dummy/file.txt'
      expect { downloader.check_md5(filename, @checksum, @remote_filename, false) }.to raise_error(Exception)
    end
  end

  describe '#create_link_file' do
    it 'Creates link file' do
      path = 'remote_path'
      output = downloader.create_link_file(path)
      expect(output).to be_instance_of(LinkFile)
      expect(output.files.first['file']).to eq path
    end
  end

  describe '#save_custom_metadata' do
    it 'Saves optional entity fields' do
      file_hash = { 'feed' => 'feed', 'custom' => 'custom' }
      downloader.save_custom_metadata(entity, file_hash)
      expect(entity.runtime['global']['feed']).not_to eq file_hash['feed']
      expect(entity.runtime['global']['custom']).to eq file_hash['custom']
    end
  end

  describe '#attach_runtime_metadata' do
    it 'Raises ArgumentError for wrong args' do
      expect { downloader.attach_runtime_metadata(nil, file: {}) }.to raise_error ArgumentError
      expect { downloader.attach_runtime_metadata(Object.new, {}) }.to raise_error ArgumentError
    end

    it 'Saves runtime metadata to entity' do
      entity.custom = {}
      file_hash = {
        remote_filename: REMOTE_FOLDER + '/event.csv',
        local_filename: 'source/event.csv',
        entity_name: 'Event',
        batch: '1471888584__batch.json',
        now: Time.now,
        file:         { path: REMOTE_FOLDER + '/event.csv',
                        timestamp: '1471888590',
                        version: 'default',
                        number_of_rows: '4',
                        hash: 'unknown',
                        export_type: 'inc',
                        sequence: nil,
                        regex: 'manifest',
                        row: 'csv_row' }
      }
      allow(downloader).to receive(:save_custom_metadata).with(entity, file_hash[:file][:row])
      expect(downloader).to receive(:save_custom_metadata)
      downloader.attach_runtime_metadata(entity, file_hash)
      expect(entity.runtime['global']['manifest_timestamp']).to eq file_hash[:file][:timestamp]
      expect(entity.runtime['global']['manifest_version']).to eq file_hash[:file][:version]
      expect(entity.runtime['global']['num_rows']).to eq file_hash[:file][:number_of_rows]
      expect(entity.runtime['global']['hash']).to eq file_hash[:file][:hash]
      expect(entity.runtime['global']['batch']).to eq file_hash[:batch]
      expect(entity.runtime['global']['sequence']).to eq nil
      expect(entity.runtime['global']['regex']).to eq file_hash[:file][:regex]
    end
  end

  describe '#initialize_batches' do
    it 'Initializes if manifest_process_type = move' do
      GoodData::Connectors::DownloaderCsv::S3Helper.copy_batch_folder(REMOTE_TESTING_FOLDER)

      batch_id = REMOTE_TESTING_FOLDER
      manifest_process_type = 'move'
      output = downloader.initialize_batches(manifest_process_type, batch_id)

      expect(output[:all_batches]).to be_a_kind_of(Array)
      expect(output[:all_batches].empty?).to be_truthy

      expect(output[:previous_batch]).to be_a_kind_of(Batch)
      expect(output[:previous_batch].sequence).to eq 2
    end

    it 'Initializes if manifest_process_type = history' do
      GoodData::Connectors::DownloaderCsv::S3Helper.copy_batch_folder(REMOTE_TESTING_FOLDER)

      batch_id = REMOTE_TESTING_FOLDER
      manifest_process_type = 'history'
      output = downloader.initialize_batches(manifest_process_type, batch_id)

      expect(output[:all_batches]).to be_a_kind_of(Array)
      expect(output[:all_batches].empty?).to be_falsey
      expect(output[:all_batches].last).to be_a_kind_of(Batch)

      expect(output[:previous_batch]).to be_a_kind_of(Batch)
      expect(output[:previous_batch].sequence).to eq 2
    end
  end
end
