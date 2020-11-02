require 'gdc_etl_csv_downloader'
require 'pry'

describe 'Downloading data from S3 and preparing metadata, batch and files for ads integrator', type: :feature do
  before :all do
    Csv = GoodData::Connectors::DownloaderCsv::Csv
    Metadata = GoodData::Connectors::Metadata::Metadata
    ConnectionHelper = GoodData::Connectors::DownloaderCsv::ConnectionHelper
    IntegrationHelper = GoodData::Connectors::DownloaderCsv::IntegrationHelper
    S3Helper = GoodData::Connectors::DownloaderCsv::S3Helper
    RuntimeMetadataValidator = GoodData::Connectors::DownloaderCsv::RuntimeMetadataValidator
    BatchValidator = GoodData::Connectors::DownloaderCsv::BatchValidator
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
    @padding = 'LeQTVzJG'
    allow(SecureRandom).to receive(:urlsafe_base64).with(6).and_return(@padding)
  end

  after :each do
    FileUtils.rm_rf('tmp')
    FileUtils.rm_rf('metadata')
    FileUtils.rm_rf('source')
  end

  it 'prepares data from uncompressed file with manifest and without feed files' do
    remote_testing_folder = S3Helper.generate_testing_folder_name
    con_params = S3Helper.build_connection_params(remote_testing_folder)
    replacement = {
        '$REMOTE_TESTING_FOLDER' => remote_testing_folder
    }
    expected_batch_pattern = %r(\{\n\ \ "identification":\ "csv_downloader_1",\n\ \ "sequence":\ 1,\n\ \ "filename":\ "manifest_1\.20160714105302\.csv",\n\ \ "files":\ \[\n\ \ \ \ \{\n\ \ \ \ \ \ "entity":\ "Event",\n\ \ \ \ \ \ "file":\ "ETL_tests\/connectors_downloader_csv\/#{remote_testing_folder}\/#{ConnectionHelper::DEFAULT_DOWNLOADER}\/Event\/#{Regexp.quote(IntegrationHelper.time_path)}\/\d{16}_data_#{@padding}\.csv"\n\ \ \ \ \}\n\ \ \]\n\})
    expected_metadata = IntegrationHelper.loadMetadataFile('spec/data/files/feature/expected_metadata_1.json')

    remote_config_path = S3Helper.generate_remote_path('configuration.json', remote_testing_folder)
    S3Helper.upload_file('spec/data/configurations/s3/configuration_7.json', remote_config_path, replacement)
    remote_data_path = S3Helper.generate_remote_path('data_files/events0.csv', remote_testing_folder)
    S3Helper.upload_file('spec/data/files/feature/events0.csv', remote_data_path)
    manifest_path = S3Helper.generate_remote_path('manifests/manifest_1.20160714105302.csv', remote_testing_folder)
    S3Helper.upload_file('spec/data/files/feature/manifest_1.20160714105302.csv', manifest_path, replacement)

    metadata = Metadata.new(con_params)
    downloader = Csv.new(metadata, con_params)
    downloader.execute(con_params)

    batch_path = S3Helper.generate_remote_path(S3Helper.get_batches_folder('batches'), remote_testing_folder) + IntegrationHelper.time_path
    S3Helper.download_files(batch_path, 'tmp/Event/')
    metadata_path = S3Helper.generate_remote_path(S3Helper.get_metadata_folder('metadata/Event'), remote_testing_folder) + IntegrationHelper.time_path
    S3Helper.download_files(metadata_path, 'tmp/Event/')
    files_path = S3Helper.generate_remote_path(S3Helper.get_data_folder('Event'), remote_testing_folder) + IntegrationHelper.time_path
    S3Helper.download_files(files_path, 'tmp/Event/')

    batch = File.open(Dir['tmp/Event/*_batch.json'].first).read
    data = File.open(Dir["tmp/Event/*_data_#{@padding}.csv"].first).read
    metadata = File.open(Dir['tmp/Event/*_metadata.json'].first).read

    expect(batch).to match(expected_batch_pattern)
    expect(data).to eq File.open('spec/data/files/feature/events0.csv').read
    expect(metadata).to eq expected_metadata
    GoodData::Connectors::DownloaderCsv::S3Helper.clear_bucket(remote_testing_folder)
  end

  it 'prepares data from uncompressed file without manifest and feed files' do
    remote_testing_folder = S3Helper.generate_testing_folder_name
    con_params = S3Helper.build_connection_params(remote_testing_folder)
    replacement = {
        '$REMOTE_TESTING_FOLDER' => remote_testing_folder
    }
    expected_batch_pattern = %r(\{\n\ \ "identification":\ "csv_downloader_1",\n\ \ "sequence":\ null,\n\ \ "filename":\ "manifest\-#{Time.now.strftime("%Y") + Time.now.strftime("%m") + Time.now.strftime("%d")}\d{6}",\n\ \ "files":\ \[\n\ \ \ \ \{\n\ \ \ \ \ \ "entity":\ "Event",\n\ \ \ \ \ \ "file":\ "ETL_tests\/connectors_downloader_csv\/#{remote_testing_folder}\/#{ConnectionHelper::DEFAULT_DOWNLOADER}\/Event\/#{Regexp.quote(IntegrationHelper.time_path)}\/\d{16}_data_#{@padding}\.csv"\n\ \ \ \ \}\n\ \ \]\n\})
    expected_metadata = IntegrationHelper.loadMetadataFile('spec/data/files/feature/expected_metadata.json')

    remote_config_path = S3Helper.generate_remote_path('configuration.json', remote_testing_folder)
    S3Helper.upload_file('spec/data/configurations/s3/configuration_1.json', remote_config_path, replacement)
    remote_data_path = S3Helper.generate_remote_path('data_files/events.csv', remote_testing_folder)
    S3Helper.upload_file('spec/data/files/feature/events.csv', remote_data_path)

    metadata = Metadata.new(con_params)
    downloader = Csv.new(metadata, con_params)
    downloader.execute(con_params)

    batch_path = S3Helper.generate_remote_path(S3Helper.get_batches_folder('batches'), remote_testing_folder) + IntegrationHelper.time_path
    S3Helper.download_files(batch_path, 'tmp/')
    metadata_path = S3Helper.generate_remote_path(S3Helper.get_metadata_folder('metadata/Event'), remote_testing_folder) + IntegrationHelper.time_path
    S3Helper.download_files(metadata_path, 'tmp/Event/')
    files_path = S3Helper.generate_remote_path(S3Helper.get_data_folder('Event'), remote_testing_folder) + IntegrationHelper.time_path
    S3Helper.download_files(files_path, 'tmp/Event/')

    batch = File.open(Dir['tmp/*_batch.json'].first).read
    data = File.open(Dir["tmp/Event/*_data_#{@padding}.csv"].first).read
    metadata = File.open(Dir['tmp/Event/*_metadata.json'].first).read

    #batch_validator = BatchValidator.new('csv_downloader_1', nil, %r(manifest\-#{Time.now.strftime("%Y") + Time.now.strftime("%m") + Time.now.strftime("%d")}\d{6}), {'Event' => 1})
    #expect(batch_validator.validate(batch)).to eq true
    expect(batch).to match(expected_batch_pattern)
    expect(data).to eq File.open('spec/data/files/feature/events.csv').read
    expect(metadata).to eq expected_metadata
    GoodData::Connectors::DownloaderCsv::S3Helper.clear_bucket(remote_testing_folder)
  end

  it 'prepares data from compressed file with manifests and feed files (x2)' do
    remote_testing_folder = S3Helper.generate_testing_folder_name
    con_params = S3Helper.build_connection_params(remote_testing_folder)
    replacement = {
        '$REMOTE_TESTING_FOLDER' => remote_testing_folder
    }
    expected_batch_pattern = %r(\{\n\ \ "identification":\ "csv_downloader_1",\n\ \ "sequence":\ 2,\n\ \ "filename":\ "manifest_2\.20160914105300\.csv",\n\ \ "files":\ \[\n\ \ \ \ \{\n\ \ \ \ \ \ "entity":\ "Event",\n\ \ \ \ \ \ "file":\ "ETL_tests\/connectors_downloader_csv\/#{remote_testing_folder}\/#{ConnectionHelper::DEFAULT_DOWNLOADER}\/Event\/#{Regexp.quote(IntegrationHelper.time_path)}\/\d{16}_data_#{@padding}\.gz"\n\ \ \ \ \}\n\ \ \]\n\})
    expected_metadata = IntegrationHelper.loadMetadataFile('spec/data/files/feature/expected_metadata_7.json')

    remote_config_path = S3Helper.generate_remote_path('configuration.json', remote_testing_folder)
    S3Helper.upload_file('spec/data/configurations/s3/configuration_2.json', remote_config_path, replacement)
    remote_data_path = S3Helper.generate_remote_path('data_files/events.gz', remote_testing_folder)
    S3Helper.upload_file('spec/data/files/feature/events0.gz', remote_data_path)
    manifest_path = S3Helper.generate_remote_path('manifests/manifest_1.20160714105300.csv', remote_testing_folder)
    S3Helper.upload_file('spec/data/files/feature/manifest_1.20160714105300.csv', manifest_path, replacement)
    feed_path = S3Helper.generate_remote_path('feed', remote_testing_folder)
    S3Helper.upload_file('spec/data/files/feature/feed', feed_path)

    metadata = Metadata.new(con_params)
    downloader = Csv.new(metadata, con_params)
    downloader.execute(con_params)

    manifest_path = S3Helper.generate_remote_path('manifests/manifest_2.20160914105300.csv', remote_testing_folder)
    S3Helper.upload_file('spec/data/files/feature/manifest_2.20160914105300.csv', manifest_path, replacement)
    S3Helper.upload_file('spec/data/files/feature/events.gz', remote_data_path)
    S3Helper.upload_file('spec/data/files/feature/feed4', feed_path)
    downloader.execute(con_params)

    batch_path = S3Helper.generate_remote_path(S3Helper.get_batches_folder('batches'), remote_testing_folder) + IntegrationHelper.time_path
    S3Helper.download_files(batch_path, 'tmp/')
    metadata_path = S3Helper.generate_remote_path(S3Helper.get_metadata_folder('metadata/Event'), remote_testing_folder) + IntegrationHelper.time_path
    S3Helper.download_files(metadata_path, 'tmp/Event/')
    files_path = S3Helper.generate_remote_path(S3Helper.get_data_folder('Event'), remote_testing_folder) + IntegrationHelper.time_path
    S3Helper.download_files(files_path, 'tmp/Event/')

    batch = File.open(Dir['tmp/*_2_batch.json'].first).read
    data = File.open(Dir["tmp/Event/*_data_#{@padding}.gz"].sort_by { |file| file.split('/')[-1].split('_')[0].to_i}.last)
    metadata = File.open(Dir['tmp/Event/*_metadata.json'].sort_by { |file| file.split('/')[-1].split('_')[0].to_i}.last).read

    #batch_validator = BatchValidator.new('csv_downloader_1', 2, %r(manifest_2\.20160914105300\.csv), {'Event' => 1}, 'gz')
    #expect(batch_validator.validate(batch)).to eq true
    expect(batch).to match(expected_batch_pattern)
    expect(FileUtils.compare_file(data, File.open('spec/data/files/feature/events.gz'))).to be true
    expect(metadata).to eq expected_metadata
    GoodData::Connectors::DownloaderCsv::S3Helper.clear_bucket(remote_testing_folder)
  end

  it 'prepares data from uncompressed file with manifests and feed files, three runs, first full, other two inc' do
    remote_testing_folder = S3Helper.generate_testing_folder_name
    con_params = S3Helper.build_connection_params(remote_testing_folder)
    replacement = {
        '$REMOTE_TESTING_FOLDER' => remote_testing_folder
    }
    extra1_padding = 'kXt0ZbFk'
    extra2_padding = 'aXt0AbFz'
    allow(SecureRandom).to receive(:urlsafe_base64).with(6).and_return(@padding,extra1_padding,extra2_padding)
    expected_batch_pattern = %r(\{\n\ \ "identification":\ "csv_downloader_1",\n\ \ "sequence":\ 3,\n\ \ "filename":\ "manifest_3\.20160714105302\.csv",\n\ \ "files":\ \[\n\ \ \ \ \{\n\ \ \ \ \ \ "entity":\ "Event",\n\ \ \ \ \ \ "file":\ "ETL_tests\/connectors_downloader_csv\/#{remote_testing_folder}\/#{ConnectionHelper::DEFAULT_DOWNLOADER}\/Event\/#{Regexp.quote(IntegrationHelper.time_path)}\/\d{16}_data_#{extra2_padding}\.csv"\n\ \ \ \ \}\n\ \ \]\n\})
    expected_metadata = IntegrationHelper.loadMetadataFile('spec/data/files/feature/expected_metadata_5.json')
    expected_runtime_metadata1 = %r(\{"global"=>"\{\\"value\\":\{\\"start_date\\":null,\\"downloader_id\\":\\"#{ConnectionHelper::DEFAULT_DOWNLOADER}\\",\\"metadata_file\\":\\"\d{16}_metadata\.json\\",\\"metadata_date\\":\{\\"year\\":\\"#{Time.now.strftime('%Y')}\\",\\"month\\":\\"#{Time.now.strftime('%m')}\\",\\"day\\":\\"#{Time.now.strftime('%d')}\\",\\"timestamp\\":\\"\d{16}\\"\},\\"manifest_timestamp\\":\\"1468486380\\",\\"manifest_version\\":\\"1\.0\\",\\"downloader_version\\":\\"#{GoodData::Connectors::DownloaderCsv::VERSION}\\",\\"num_rows\\":null,\\"hash\\":\\"unknown\\",\\"batch\\":\\"\d{16}_1_batch\.json\\",\\"full\\":true,\\"sequence\\":1,\\"target_ads\\":null,\\"md5\\":\\"unknown\\",\\"export_type\\":\\"full\\",\\"source_filename\\":\\"source\/events0\.csv\\",\\"original_filename\\":\\"events0\.csv\\"\}\}"\})
    expected_runtime_metadata2 = %r(\{"global"=>"\{\\"value\\":\{\\"start_date\\":null,\\"downloader_id\\":\\"#{ConnectionHelper::DEFAULT_DOWNLOADER}\\",\\"metadata_file\\":\\"\d{16}_metadata\.json\\",\\"metadata_date\\":\{\\"year\\":\\"#{Time.now.strftime('%Y')}\\",\\"month\\":\\"#{Time.now.strftime('%m')}\\",\\"day\\":\\"#{Time.now.strftime('%d')}\\",\\"timestamp\\":\\"\d{16}\\"\},\\"manifest_timestamp\\":\\"1468486380\\",\\"manifest_version\\":\\"1\.0\\",\\"downloader_version\\":\\"#{GoodData::Connectors::DownloaderCsv::VERSION}\\",\\"num_rows\\":null,\\"hash\\":\\"unknown\\",\\"batch\\":\\"\d{16}_2_batch\.json\\",\\"sequence\\":2,\\"target_ads\\":null,\\"md5\\":\\"unknown\\",\\"export_type\\":\\"inc\\",\\"source_filename\\":\\"source\/events\.csv\\",\\"original_filename\\":\\"events\.csv\\"\}\}"\})
    expected_runtime_metadata3 = %r(\{"global"=>"\{\\"value\\":\{\\"start_date\\":null,\\"downloader_id\\":\\"#{ConnectionHelper::DEFAULT_DOWNLOADER}\\",\\"metadata_file\\":\\"\d{16}_metadata\.json\\",\\"metadata_date\\":\{\\"year\\":\\"#{Time.now.strftime('%Y')}\\",\\"month\\":\\"#{Time.now.strftime('%m')}\\",\\"day\\":\\"#{Time.now.strftime('%d')}\\",\\"timestamp\\":\\"\d{16}\\"\},\\"manifest_timestamp\\":\\"1468486380\\",\\"manifest_version\\":\\"1\.0\\",\\"downloader_version\\":\\"#{GoodData::Connectors::DownloaderCsv::VERSION}\\",\\"num_rows\\":null,\\"hash\\":\\"unknown\\",\\"batch\\":\\"\d{16}_3_batch\.json\\",\\"sequence\\":3,\\"target_ads\\":null,\\"md5\\":\\"unknown\\",\\"export_type\\":\\"inc\\",\\"source_filename\\":\\"source\/events1\.csv\\",\\"original_filename\\":\\"events1\.csv\\"\}\}"\})

    runtime_metadata_options = JSON.parse(DEFAULT_RUNTIME_METADATA_OPTIONS.to_json)
    runtime_metadata_options['eq_options']['manifest_version'] = '1.0'

    remote_config_path = S3Helper.generate_remote_path('configuration.json', remote_testing_folder)
    S3Helper.upload_file('spec/data/configurations/s3/configuration_5.json', remote_config_path, replacement)
    remote_data_path = S3Helper.generate_remote_path('data_files/events0.csv', remote_testing_folder)
    S3Helper.upload_file('spec/data/files/feature/events0.csv', remote_data_path)
    remote_data_path = S3Helper.generate_remote_path('data_files/events.csv', remote_testing_folder)
    S3Helper.upload_file('spec/data/files/feature/events.csv', remote_data_path)
    remote_data_path = S3Helper.generate_remote_path('data_files/events1.csv', remote_testing_folder)
    S3Helper.upload_file('spec/data/files/feature/events1.csv', remote_data_path)
    manifest_path = S3Helper.generate_remote_path('manifests/manifest_1.20160714105302.csv', remote_testing_folder)
    S3Helper.upload_file('spec/data/files/feature/manifest_1.20160714105302.csv', manifest_path, replacement)
    manifest_path = S3Helper.generate_remote_path('manifests/manifest_2.20160714105302.csv', remote_testing_folder)
    S3Helper.upload_file('spec/data/files/feature/manifest_2.20160714105302.csv', manifest_path, replacement)
    manifest_path = S3Helper.generate_remote_path('manifests/manifest_3.20160714105302.csv', remote_testing_folder)
    S3Helper.upload_file('spec/data/files/feature/manifest_3.20160714105302.csv', manifest_path, replacement)
    feed_path = S3Helper.generate_remote_path('feed', remote_testing_folder)
    S3Helper.upload_file('spec/data/files/feature/feed', feed_path)

    metadata = Metadata.new(con_params)
    downloader = Csv.new(metadata, con_params)
    downloader.execute(con_params)

    batch_path = S3Helper.generate_remote_path(S3Helper.get_batches_folder('batches'), remote_testing_folder) + IntegrationHelper.time_path
    S3Helper.download_files(batch_path, 'tmp/')
    metadata_path = S3Helper.generate_remote_path(S3Helper.get_metadata_folder('metadata/Event'), remote_testing_folder) + IntegrationHelper.time_path
    S3Helper.download_files(metadata_path, 'tmp/Event/')
    files_path = S3Helper.generate_remote_path(S3Helper.get_data_folder('Event'), remote_testing_folder) + IntegrationHelper.time_path
    S3Helper.download_files(files_path, 'tmp/Event/')

    batch1 = File.open(Dir['tmp/*_1_batch.json'].first).read
    batch2 = File.open(Dir['tmp/*_2_batch.json'].first).read
    batch3 = File.open(Dir['tmp/*_3_batch.json'].first).read
    data = File.open(Dir["tmp/Event/*_data_#{extra2_padding}.csv"].sort.last)
    metadata = File.open(Dir['tmp/Event/*_metadata.json'].sort.last).read

    runtime_metadata_options['eq_options']['full'] = true
    runtime_metadata_options['match_options']['batch'] = %r(\d{16}_1_batch\.json)
    runtime_metadata_options['eq_options']['sequence'] = 1
    runtime_metadata_options['eq_options']['source_filename'] = 'source/events0.csv'
    runtime_metadata_options['eq_options']['original_filename'] = 'events0.csv'
    runtime_metadata_validator = RuntimeMetadataValidator.new(runtime_metadata_options)
    expect(runtime_metadata_validator.validate(get_metadata(batch1))).to eq true
    runtime_metadata_options['eq_options'].delete('full')


    runtime_metadata_options['match_options']['batch'] = %r(\d{16}_2_batch\.json)
    runtime_metadata_options['eq_options']['sequence'] = 2
    runtime_metadata_options['eq_options']['source_filename'] = 'source/events.csv'
    runtime_metadata_options['eq_options']['original_filename'] = 'events.csv'
    runtime_metadata_validator = RuntimeMetadataValidator.new(runtime_metadata_options)
    expect(runtime_metadata_validator.validate(get_metadata(batch2))).to eq true

    runtime_metadata_options['match_options']['batch'] = %r(\d{16}_3_batch\.json)
    runtime_metadata_options['eq_options']['sequence'] = 3
    runtime_metadata_options['eq_options']['source_filename'] = 'source/events1.csv'
    runtime_metadata_options['eq_options']['original_filename'] = 'events1.csv'
    runtime_metadata_validator = RuntimeMetadataValidator.new(runtime_metadata_options)
    expect(runtime_metadata_validator.validate(get_metadata(batch3))).to eq true

    #batch_validator = BatchValidator.new('csv_downloader_1', 3, %r(manifest_3\.20160714105302\.csv), {'Event' => 1})
    #expect(batch_validator.validate(batch3)).to eq true
    expect(S3Helper.get_metadata(batch1)).to match expected_runtime_metadata1
    expect(S3Helper.get_metadata(batch2)).to match expected_runtime_metadata2
    expect(S3Helper.get_metadata(batch3)).to match expected_runtime_metadata3

    expect(batch3).to match(expected_batch_pattern)
    expect(FileUtils.compare_file(data, File.open('spec/data/files/feature/events1.csv'))).to be true
    expect(metadata).to eq expected_metadata
    GoodData::Connectors::DownloaderCsv::S3Helper.clear_bucket(remote_testing_folder)
  end

  it 'prepares data from xlsx file without manifests with feed file (City full)' do
    remote_testing_folder = S3Helper.generate_testing_folder_name
    con_params = S3Helper.build_connection_params(remote_testing_folder)
    replacement = {
        '$REMOTE_TESTING_FOLDER' => remote_testing_folder
    }
    expected_batch_pattern = %r(\{\n\ \ "identification":\ "csv_downloader_1",\n\ \ "sequence":\ null,\n\ \ "filename":\ "manifest\-#{Time.now.strftime("%Y") + Time.now.strftime("%m") + Time.now.strftime("%d")}\d{6}",\n\ \ "files":\ \[\n\ \ \ \ \{\n\ \ \ \ \ \ "entity":\ "City",\n\ \ \ \ \ \ "file":\ "ETL_tests\/connectors_downloader_csv\/#{remote_testing_folder}\/#{ConnectionHelper::DEFAULT_DOWNLOADER}\/City\/#{Regexp.quote(IntegrationHelper.time_path)}\/\d{16}_data_#{@padding}\.csv"\n\ \ \ \ \},\n\ \ \ \ \{\n\ \ \ \ \ \ "entity":\ "Zombie",\n\ \ \ \ \ \ "file":\ "ETL_tests\/connectors_downloader_csv\/#{remote_testing_folder}\/#{ConnectionHelper::DEFAULT_DOWNLOADER}\/Zombie\/#{Regexp.quote(IntegrationHelper.time_path)}\/\d{16}_data_#{@padding}\.csv"\n\ \ \ \ \}\n\ \ \]\n\})
    expected_batch_pattern2 = %r(\{\n\ \ "identification":\ "csv_downloader_1",\n\ \ "sequence":\ null,\n\ \ "filename":\ "manifest\-#{Time.now.strftime("%Y") + Time.now.strftime("%m") + Time.now.strftime("%d")}\d{6}",\n\ \ "files":\ \[\n\ \ \ \ \{\n\ \ \ \ \ \ "entity":\ "Zombie",\n\ \ \ \ \ \ "file":\ "ETL_tests\/connectors_downloader_csv\/#{remote_testing_folder}\/#{ConnectionHelper::DEFAULT_DOWNLOADER}\/Zombie\/#{Regexp.quote(IntegrationHelper.time_path)}\/\d{16}_data_#{@padding}\.csv"\n\ \ \ \ \},\n\ \ \ \ \{\n\ \ \ \ \ \ "entity":\ "City",\n\ \ \ \ \ \ "file":\ "ETL_tests\/connectors_downloader_csv\/#{remote_testing_folder}\/#{ConnectionHelper::DEFAULT_DOWNLOADER}\/City\/#{Regexp.quote(IntegrationHelper.time_path)}\/\d{16}_data_#{@padding}\.csv"\n\ \ \ \ \}\n\ \ \]\n\})
    expected_metadata = IntegrationHelper.loadMetadataFile('spec/data/files/feature/expected_metadata_3.json')
    expected_runtime_metadata = %r(\{"global"=>"\{\\"value\\":\{\\"start_date\\":null,\\"downloader_id\\":\\"#{ConnectionHelper::DEFAULT_DOWNLOADER}\\",\\"metadata_file\\":\\"\d{16}_metadata\.json\\",\\"metadata_date\\":\{\\"year\\":\\"#{Time.now.strftime('%Y')}\\",\\"month\\":\\"#{Time.now.strftime('%m')}\\",\\"day\\":\\"#{Time.now.strftime('%d')}\\",\\"timestamp\\":\\"\d{16}\\"\},\\"manifest_timestamp\\":\\"\d{10}\\",\\"manifest_version\\":\\"default\\",\\"downloader_version\\":\\"#{GoodData::Connectors::DownloaderCsv::VERSION}\\",\\"num_rows\\":\\"\\",\\"hash\\":\\"unknown\\",\\"batch\\":\\"\d{16}__batch\.json\\",\\"full\\":true,\\"target_ads\\":\\"\\",\\"md5\\":\\"unknown\\",\\"target_predicate\\":null,\\"export_type\\":\\"full\\",\\"source_filename\\":\\"source\/\d{10}_City\.csv\\",\\"original_filename\\":\\"\d{10}_City\.csv\\"\}\}"\})

    runtime_metadata_options = JSON.parse(DEFAULT_RUNTIME_METADATA_OPTIONS.to_json)
    runtime_metadata_options['eq_options']['num_rows'] = ''
    runtime_metadata_options['eq_options']['full'] = true
    runtime_metadata_options['match_options']['original_filename'] = %r{\d{10}_City.csv}
    runtime_metadata_options['match_options']['source_filename'] = %r{source\/\d{10}_City.csv}
    runtime_metadata_validator = RuntimeMetadataValidator.new(runtime_metadata_options)

    remote_config_path = S3Helper.generate_remote_path('configuration.json', remote_testing_folder)
    S3Helper.upload_file('spec/data/configurations/s3/configuration_3.json', remote_config_path, replacement)
    remote_data_folder = S3Helper.generate_remote_path('data_files/', remote_testing_folder)
    remote_data_path = remote_data_folder + 'entities.xlsx'
    S3Helper.upload_file('spec/data/files/data_folder/entities.xlsx', remote_data_path)
    feed_path = S3Helper.generate_remote_path('feed', remote_testing_folder)
    S3Helper.upload_file('spec/data/files/feature/feed2', feed_path)

    metadata = Metadata.new(con_params.merge('csv|options|ignore_columns_check'=>true))
    downloader = Csv.new(metadata, con_params)
    GoodData::Connectors::XlsxToCsvMiddleWare.new.call(con_params.merge('metadata_wrapper'=>metadata)) rescue nil
    con_params[:entities_xlsx] = [remote_data_path]
    downloader.execute(con_params)

    batch_path = S3Helper.generate_remote_path(S3Helper.get_batches_folder('batches'), remote_testing_folder) + IntegrationHelper.time_path
    S3Helper.download_files(batch_path, 'tmp/City/')
    # We compare only data and metadata for first entity
    metadata_path = S3Helper.generate_remote_path(S3Helper.get_metadata_folder('metadata/City'), remote_testing_folder) + IntegrationHelper.time_path
    S3Helper.download_files(metadata_path, 'tmp/City/')
    files_path = S3Helper.generate_remote_path(S3Helper.get_data_folder('City'), remote_testing_folder) + IntegrationHelper.time_path
    S3Helper.download_files(files_path, 'tmp/City/')

    batch = File.read(Dir['tmp/City/*_batch.json'].first)
    data = File.open(Dir["tmp/City/*_data_#{@padding}.csv"].sort_by { |file| file.split('/')[-1].split('_')[0].to_i}.first)
    metadata = File.read(Dir['tmp/City/*_metadata.json'].sort_by { |file| file.split('/')[-1].split('_')[0].to_i}.last)

    #batch_validator = BatchValidator.new('csv_downloader_1', nil, %r(manifest\-#{Time.now.strftime("%Y") + Time.now.strftime("%m") + Time.now.strftime("%d")}\d{6}), {'City' => 1, 'Zombie' => 1})
    #expect(batch_validator.validate(batch)).to eq true
    expect(S3Helper.get_metadata(batch, 'City')).to match expected_runtime_metadata
    expect(runtime_metadata_validator.validate(get_metadata(batch, 'City'))).to eq true
    # Batch may have random order of entities, so rather than overcomplicating regexp we match data against both our expected patterns
    expect(batch.match(expected_batch_pattern) || batch.match(expected_batch_pattern2)).to be_truthy

    move_files = S3Helper.list_files(remote_data_folder + 'data_processed')
    expect(move_files.length).to eq 3
    move_files.each do |file|
      expect(file.include?('City') || file.include?('zombie') || file.include?('entities.xlsx')).to eq true
    end

    GoodData::Connectors::DownloaderCsv::S3Helper.clear_bucket(remote_testing_folder)
  end

  it 'prepares data from uncompressed file without manifest and feed files with entity_name parsing' do
    remote_testing_folder = S3Helper.generate_testing_folder_name
    con_params = S3Helper.build_connection_params(remote_testing_folder)
    replacement = {
        '$REMOTE_TESTING_FOLDER' => remote_testing_folder
    }
    expected_batch_pattern = %r(\{\n\ \ "identification":\ "csv_downloader_1",\n\ \ "sequence":\ null,\n\ \ "filename":\ "manifest\-#{Time.now.strftime("%Y") + Time.now.strftime("%m") + Time.now.strftime("%d")}\d{6}",\n\ \ "files":\ \[\n\ \ \ \ \{\n\ \ \ \ \ \ "entity":\ "Event",\n\ \ \ \ \ \ "file":\ "ETL_tests\/connectors_downloader_csv\/#{remote_testing_folder}\/#{ConnectionHelper::DEFAULT_DOWNLOADER}\/Event\/#{Regexp.quote(IntegrationHelper.time_path)}\/\d{16}_data_#{@padding}\.csv"\n\ \ \ \ \}\n\ \ \]\n\})
    expected_metadata = IntegrationHelper.loadMetadataFile('spec/data/files/feature/expected_metadata.json')
    expected_runtime_metadata = %r(\{"global"=>"\{\\"value\\":\{\\"start_date\\":null,\\"downloader_id\\":\\"#{ConnectionHelper::DEFAULT_DOWNLOADER}\\",\\"metadata_file\\":\\"\d{16}_metadata\.json\\",\\"metadata_date\\":\{\\"year\\":\\"#{Time.now.strftime('%Y')}\\",\\"month\\":\\"#{Time.now.strftime('%m')}\\",\\"day\\":\\"#{Time.now.strftime('%d')}\\",\\"timestamp\\":\\"\d{16}\\"\},\\"manifest_timestamp\\":\\"\d{10}\\",\\"manifest_version\\":\\"default\\",\\"downloader_version\\":\\"#{GoodData::Connectors::DownloaderCsv::VERSION}\\",\\"num_rows\\":\\"\\",\\"hash\\":\\"unknown\\",\\"batch\\":\\"\d{16}__batch\.json\\",\\\"full\\\":true,\\"entity_regex\\":\\"R\.pd\\",\\\"target_ads\\\":\\\"\\\",\\\"md5\\\":\\\"unknown\\\",\\\"target_predicate\\\":null,\\\"export_type\\\":\\\"full\\\",\\"source_filename\\":\\"source\/Event_20160914_R\.pd\.csv\\",\\"original_filename\\":\\"Event_20160914_R\.pd\.csv\\"\}\}"\})

    runtime_metadata_options = JSON.parse(DEFAULT_RUNTIME_METADATA_OPTIONS.to_json)
    runtime_metadata_options['eq_options']['num_rows'] = ''
    runtime_metadata_options['eq_options']['entity_regex'] = 'R.pd'
    runtime_metadata_options['eq_options']['original_filename'] = 'Event_20160914_R.pd.csv'
    runtime_metadata_options['eq_options']['source_filename'] = 'source/Event_20160914_R.pd.csv'
    runtime_metadata_validator = RuntimeMetadataValidator.new(runtime_metadata_options)

    remote_config_path = S3Helper.generate_remote_path('configuration.json', remote_testing_folder)
    S3Helper.upload_file('spec/data/configurations/s3/configuration_6.json', remote_config_path, replacement)
    remote_data_path = S3Helper.generate_remote_path('data_files/Event_20160914_R.pd.csv', remote_testing_folder)
    S3Helper.upload_file('spec/data/files/feature/events.csv', remote_data_path)

    metadata = Metadata.new(con_params)
    downloader = Csv.new(metadata, con_params)
    downloader.execute(con_params)

    batch_path = S3Helper.generate_remote_path(S3Helper.get_batches_folder('batches'), remote_testing_folder) + IntegrationHelper.time_path
    S3Helper.download_files(batch_path, 'tmp/Event/')
    metadata_path = S3Helper.generate_remote_path(S3Helper.get_metadata_folder('metadata/Event'), remote_testing_folder) + IntegrationHelper.time_path
    S3Helper.download_files(metadata_path, 'tmp/Event/')
    files_path = S3Helper.generate_remote_path(S3Helper.get_data_folder('Event'), remote_testing_folder) + IntegrationHelper.time_path
    S3Helper.download_files(files_path, 'tmp/Event/')

    batch = File.open(Dir['tmp/Event/*_batch.json'].first).read
    data = File.open(Dir["tmp/Event/*_data_#{@padding}.csv"].first).read
    metadata = File.open(Dir['tmp/Event/*_metadata.json'].first).read

    #batch_validator = BatchValidator.new('csv_downloader_1', nil, %r(manifest\-#{Time.now.strftime("%Y") + Time.now.strftime("%m") + Time.now.strftime("%d")}\d{6}), {'Event' => 1})
    #expect(batch_validator.validate(batch)).to eq true
    expect(runtime_metadata_validator.validate(get_metadata(batch))).to eq true
    expect(batch).to match(expected_batch_pattern)
    expect(S3Helper.get_metadata(batch)).to match expected_runtime_metadata
    expect(data).to eq File.open('spec/data/files/feature/events.csv').read
    expect(metadata).to eq expected_metadata
    GoodData::Connectors::DownloaderCsv::S3Helper.clear_bucket(remote_testing_folder)
  end

  xit 'prepares data without manifest and feed files with onedrive backend' do
    expected_batch_pattern = %r(\{\n\ \ "identification":\ "csv_downloader_1",\n\ \ "sequence":\ null,\n\ \ "filename":\ "manifest\-#{Time.now.strftime("%Y") + Time.now.strftime("%m") + Time.now.strftime("%d")}\d{6}",\n\ \ "files":\ \[\n\ \ \ \ \{\n\ \ \ \ \ \ "entity":\ "Event",\n\ \ \ \ \ \ "file":\ "ETL_tests\/connectors_downloader_csv\/#{remote_testing_folder}\/#{ConnectionHelper::DEFAULT_DOWNLOADER}\/Event\/#{Regexp.quote(IntegrationHelper.time_path)}\/\d{16}_data_#{@padding}\.csv"\n\ \ \ \ \}\n\ \ \]\n\})
    expected_metadata = IntegrationHelper.loadMetadataFile('spec/data/files/feature/expected_metadata.json')

    remote_config_path = S3Helper.generate_remote_path('configuration.json', remote_testing_folder)
    S3Helper.upload_file('spec/data/configurations/one_drive/configuration_7.json', remote_config_path, replacement)

    metadata = Metadata.new(con_params)
    downloader = Csv.new(metadata, con_params)
    downloader.execute(con_params)

    batch_path = S3Helper.generate_remote_path(S3Helper.get_batches_folder('batches'), remote_testing_folder) + IntegrationHelper.time_path
    S3Helper.download_files(batch_path, 'tmp/')
    metadata_path = S3Helper.generate_remote_path(S3Helper.get_metadata_folder('metadata/Event'), remote_testing_folder) + IntegrationHelper.time_path
    S3Helper.download_files(metadata_path, 'tmp/')
    files_path = S3Helper.generate_remote_path(S3Helper.get_data_folder('Event'), remote_testing_folder) + IntegrationHelper.time_path
    S3Helper.download_files(files_path, 'tmp/')

    batch = File.open(Dir['tmp/*_batch.json'].first).read
    data = File.open(Dir['tmp/*_data_*'].first).read
    metadata = File.open(Dir['tmp/*_metadata.json'].first).read

    #batch_validator = BatchValidator.new('csv_downloader_1', nil, %r(manifest\-#{Time.now.strftime("%Y") + Time.now.strftime("%m") + Time.now.strftime("%d")}\d{6}), {'Event' => 1})
    #expect(batch_validator.validate(batch)).to eq true
    expect(batch).to match(expected_batch_pattern)
    expect(data).to eq File.open('spec/data/files/feature/events.csv').read
    expect(metadata).to eq expected_metadata
  end

  it 'prepares data by linking to source file' do
    remote_testing_folder = S3Helper.generate_testing_folder_name
    con_params = S3Helper.build_connection_params(remote_testing_folder)
    replacement = {
        '$REMOTE_TESTING_FOLDER' => remote_testing_folder
    }

    expected_batch_pattern = %r(\{\n\ \ "identification":\ "csv_downloader_1",\n\ \ "sequence":\ null,\n\ \ "filename":\ "manifest_1510644325.csv",\n\ \ "files":\ \[\n\ \ \ \ \{\n\ \ \ \ \ \ "entity":\ "Event",\n\ \ \ \ \ \ "file":\ "ETL_tests\/connectors_downloader_csv\/#{remote_testing_folder}\/#{ConnectionHelper::DEFAULT_DOWNLOADER}\/Event\/#{Regexp.quote(IntegrationHelper.time_path)}\/\d{16}_data_#{@padding}\.json"\n\ \ \ \ \}\n\ \ \]\n\})
    expected_data_pattern= %r(\{\n  "files": \[\n    \{\n      "file": "ETL_tests\/connectors_downloader_csv\/#{remote_testing_folder}\/data_files\/events.csv"\n    \}\n  \]\n\})
    runtime_metadata_options = JSON.parse(DEFAULT_RUNTIME_METADATA_OPTIONS.to_json)
    runtime_metadata_options['eq_options']['source_filename'] = 'tmp/link.json'
    runtime_metadata_options['eq_options']['type'] = 'link'
    runtime_metadata_options['eq_options']['manifest_version'] = '1.0'
    runtime_metadata_options['match_options']['batch'] = %r(\d{16}__batch\.json)
    runtime_metadata_validator = RuntimeMetadataValidator.new(runtime_metadata_options)

    remote_config_path = S3Helper.generate_remote_path('configuration.json', remote_testing_folder)
    S3Helper.upload_file('spec/data/files/feature/use_link_file/configuration.json', remote_config_path, replacement)

    remote_data_path = S3Helper.generate_remote_path('data_files/events.csv', remote_testing_folder)
    S3Helper.upload_file('spec/data/files/feature/use_link_file/events.csv', remote_data_path)

    feed_path = S3Helper.generate_remote_path('feed', remote_testing_folder)
    S3Helper.upload_file('spec/data/files/feature/use_link_file/feed', feed_path)

    manifest_path = S3Helper.generate_remote_path('manifests/manifest_1510644325.csv', remote_testing_folder)
    S3Helper.upload_file('spec/data/files/feature/use_link_file/manifest.csv', manifest_path, replacement)

    metadata = Metadata.new(con_params)
    downloader = Csv.new(metadata, con_params)
    downloader.execute(con_params)

    batch_path = S3Helper.generate_remote_path(S3Helper.get_batches_folder('batches'), remote_testing_folder) + IntegrationHelper.time_path
    S3Helper.download_files(batch_path, 'tmp/')
    metadata_path = S3Helper.generate_remote_path(S3Helper.get_metadata_folder('metadata/Event'), remote_testing_folder) + IntegrationHelper.time_path
    S3Helper.download_files(metadata_path, 'tmp/')
    files_path = S3Helper.generate_remote_path(S3Helper.get_data_folder('Event'), remote_testing_folder) + IntegrationHelper.time_path
    S3Helper.download_files(files_path, 'tmp/')

    batch = File.open(Dir['tmp/*_batch.json'].first).read
    data = File.open(Dir["tmp/*_data_#{@padding}.json"].first).read
    metadata = File.open(Dir['tmp/*_metadata.json'].first).read

    #batch_validator = BatchValidator.new('csv_downloader_1', 1, %r(manifest_1\.20160714105300\.csv), {'Event' => 1}, 'json')
    #expect(batch_validator.validate(batch)).to eq true
    expect(runtime_metadata_validator.validate(get_metadata(batch))).to eq true
    expect(batch).to match(expected_batch_pattern)
    expect(data).to match(expected_data_pattern)
    expect(metadata).to eq IntegrationHelper.loadMetadataFile('spec/data/files/feature/use_link_file/expected_metadata.json')
    GoodData::Connectors::DownloaderCsv::S3Helper.clear_bucket(remote_testing_folder)
  end

  it 'prepares data from multi entities' do
    remote_testing_folder = S3Helper.generate_testing_folder_name
    con_params = S3Helper.build_connection_params(remote_testing_folder)
    replacement = {
        '$REMOTE_TESTING_FOLDER' => remote_testing_folder
    }

    # prepare data from real data which is saved in s3://msf-dev-grest/Do_Not_Delete_Using_In_ETL_tests/connectors_downloader_csv/AIDAJJOTCFAB7WDXNUX6A_gdc-ms-int_SVS-130/
    remote_config_path = S3Helper.generate_remote_path('configuration.json', remote_testing_folder)
    S3Helper.upload_file('spec/data/files/feature/AIDAJJOTCFAB7WDXNUX6A_gdc-ms-int_SVS-130/configuration.json', remote_config_path, replacement)

    manifest_path = S3Helper.generate_remote_path('manifests/manifest_20170608172248.csv', remote_testing_folder)
    S3Helper.upload_file('spec/data/files/feature/AIDAJJOTCFAB7WDXNUX6A_gdc-ms-int_SVS-130/manifest_20170608172248.csv', manifest_path, replacement)

    metadata = Metadata.new(con_params)
    downloader = Csv.new(metadata, con_params)
    downloader.execute(con_params)

    # verify that it is process 38 entities
    metadata_path = S3Helper.generate_remote_path(S3Helper.get_metadata_folder('metadata'), remote_testing_folder)
    expect(S3Helper.list_files(metadata_path).length).to eq 38
    files_path = S3Helper.generate_remote_path("#{ConnectionHelper::DEFAULT_DOWNLOADER}/", remote_testing_folder)
    expect(S3Helper.list_files(files_path).length).to eq 38
    GoodData::Connectors::DownloaderCsv::S3Helper.clear_bucket(remote_testing_folder)
  end

  it 'prepares data without manifest and feed files full load with timestamps' do
    remote_testing_folder = S3Helper.generate_testing_folder_name
    con_params = S3Helper.build_connection_params(remote_testing_folder)
    replacement = {
        '$REMOTE_TESTING_FOLDER' => remote_testing_folder
    }

    expected_event_metadata = IntegrationHelper.loadMetadataFile('spec/data/files/feature/expected_metadata_8.json')
    expected_opportunity_metadata = IntegrationHelper.loadMetadataFile('spec/data/files/feature/expected_metadata_6.json')

    remote_config_path = S3Helper.generate_remote_path('configuration.json', remote_testing_folder)
    S3Helper.upload_file('spec/data/configurations/s3/configuration_8.json', remote_config_path, replacement)

    remote_data_path = S3Helper.generate_remote_path('data_files/event_20170101.csv', remote_testing_folder)
    S3Helper.upload_file('spec/data/files/feature/events.csv', remote_data_path)
    remote_data_path = S3Helper.generate_remote_path('data_files/event_20170102.csv', remote_testing_folder)
    S3Helper.upload_file('spec/data/files/feature/events0.csv', remote_data_path)
    remote_data_path = S3Helper.generate_remote_path('data_files/opportunity_20170101.csv', remote_testing_folder)
    S3Helper.upload_file('spec/data/files/feature/opportunity.csv', remote_data_path)
    remote_data_path = S3Helper.generate_remote_path('data_files/opportunity_20170102.csv', remote_testing_folder)
    S3Helper.upload_file('spec/data/files/feature/opportunity0.csv', remote_data_path)

    metadata = Metadata.new(con_params)
    downloader = Csv.new(metadata, con_params)
    downloader.execute(con_params)

    batch_path = S3Helper.generate_remote_path('batches/csv_downloader_1/', remote_testing_folder) + IntegrationHelper.time_path
    S3Helper.download_files(batch_path, 'tmp/')
    batch = File.open(Dir['tmp/*_batch.json'].first).read
    #batch_validator = BatchValidator.new('csv_downloader_1', nil, %r(manifest\-#{Time.now.strftime("%Y") + Time.now.strftime("%m") + Time.now.strftime("%d")}\d{6}), {'Event' => 1, 'Opportunity' => 2})
    #expect(batch_validator.validate(batch)).to eq true


    event_metadata_path = S3Helper.generate_remote_path('metadata/Event/', remote_testing_folder) + IntegrationHelper.time_path
    S3Helper.download_files(event_metadata_path, 'tmp/event/')
    event_metadata = File.open(Dir['tmp/event/*_metadata.json'].first).read
    expect(event_metadata).to eq expected_event_metadata

    opportunity_metadata_path = S3Helper.generate_remote_path('metadata/Opportunity/', remote_testing_folder) + IntegrationHelper.time_path
    S3Helper.download_files(opportunity_metadata_path, 'tmp/opportunity/')
    opportunity_metadata = File.open(Dir['tmp/opportunity/*_metadata.json'].first).read
    expect(opportunity_metadata).to eq expected_opportunity_metadata

    event_files_path = S3Helper.generate_remote_path('csv_downloader_1/Event/', remote_testing_folder) + IntegrationHelper.time_path
    S3Helper.download_files(event_files_path, 'tmp/event/')
    event_data = File.open(Dir['tmp/event/*_data_*'].first).read
    expect(event_data).to eq File.open('spec/data/files/feature/events0.csv').read

    opportunity_files_path = S3Helper.generate_remote_path('csv_downloader_1/Opportunity/', remote_testing_folder) + IntegrationHelper.time_path
    S3Helper.download_files(opportunity_files_path, 'tmp/opportunity/')
    opportunity_data = File.open(Dir['tmp/opportunity/*_data_*'][0]).read
    opportunity_data_2 = File.open(Dir['tmp/opportunity/*_data_*'][1]).read
    expected_opportunity_data = [File.open('spec/data/files/feature/opportunity.csv').read, File.open('spec/data/files/feature/opportunity0.csv').read]
    expected_opportunity_data.each do |data|
      expect(data).to be_in [opportunity_data, opportunity_data_2]
    end

    runtime_metadata_options = JSON.parse(DEFAULT_RUNTIME_METADATA_OPTIONS.to_json)
    runtime_metadata_options['eq_options']['full'] = true
    runtime_metadata_validator = RuntimeMetadataValidator.new(runtime_metadata_options)
    expect(runtime_metadata_validator.validate(get_metadata(batch, 'Event'))).to eq true
    runtime_metadata_options = JSON.parse(DEFAULT_RUNTIME_METADATA_OPTIONS.to_json)
    runtime_metadata_options['eq_options']['full'] = nil
    runtime_metadata_validator = RuntimeMetadataValidator.new(runtime_metadata_options)
    expect(runtime_metadata_validator.validate(get_metadata(batch, 'Opportunity'))).to eq true
    GoodData::Connectors::DownloaderCsv::S3Helper.clear_bucket(remote_testing_folder)
  end

  it 'prepares data for enable field' do
    remote_testing_folder = S3Helper.generate_testing_folder_name
    con_params = S3Helper.build_connection_params(remote_testing_folder)
    replacement = {
        '$REMOTE_TESTING_FOLDER' => remote_testing_folder
    }

    feed_file_with_all_fields = 'spec/data/files/feature/feed'
    feed_file_with_disabled_fields = 'spec/data/files/feature/feed5'

    expected_disabled_fields_metadata = IntegrationHelper.loadMetadataFile('spec/data/files/feature/expected_disabled_fields_metadata.json')
    expected_enabled_fields_metadata = IntegrationHelper.loadMetadataFile('spec/data/files/feature/expected_metadata_10.json')

    # Prepare data download from CSV
    remote_config_path = S3Helper.generate_remote_path('configuration.json', remote_testing_folder)
    S3Helper.upload_file('spec/data/configurations/s3/configuration_11.json', remote_config_path, replacement)
    remote_data_path = S3Helper.generate_remote_path('data_files/events.gz', remote_testing_folder)
    S3Helper.upload_file('spec/data/files/feature/events0.gz', remote_data_path)
    manifest_path = S3Helper.generate_remote_path('manifests/manifest_1.20160714105300.csv', remote_testing_folder)
    S3Helper.upload_file('spec/data/files/feature/manifest_4.20160714105300.csv', manifest_path, replacement)
    feed_path = S3Helper.generate_remote_path('feed', remote_testing_folder)
    S3Helper.upload_file(feed_file_with_all_fields, feed_path)

    metadata = Metadata.new(con_params)
    downloader = Csv.new(metadata, con_params)
    downloader.execute(con_params)

    # Prepare data disabled fields
    manifest_path = S3Helper.generate_remote_path('manifests/manifest_2.20160714105300.csv', remote_testing_folder)
    S3Helper.upload_file('spec/data/files/feature/manifest_4.20160714105300.csv', manifest_path, replacement)
    S3Helper.upload_file(feed_file_with_disabled_fields, feed_path)

    metadata = Metadata.new(con_params)
    downloader = Csv.new(metadata, con_params)
    downloader.execute(con_params)

    # Verify disabled fields in metadata
    metadata_path = S3Helper.generate_remote_path(S3Helper.get_metadata_folder('metadata/Event'), remote_testing_folder) + IntegrationHelper.time_path
    S3Helper.download_files(metadata_path, 'tmp/Event/')
    metadata = File.open(Dir['tmp/Event/*_metadata.json'].sort_by { |file| file.split('/')[-1].split('_')[0].to_i}.last).read
    expect(metadata).to eq expected_disabled_fields_metadata

    # Prepare data enabled fields
    manifest_path = S3Helper.generate_remote_path('manifests/manifest_3.20160714105300.csv', remote_testing_folder)
    S3Helper.upload_file('spec/data/files/feature/manifest_4.20160714105300.csv', manifest_path, replacement)
    S3Helper.upload_file(feed_file_with_all_fields, feed_path)

    metadata = Metadata.new(con_params)
    downloader = Csv.new(metadata, con_params)
    downloader.execute(con_params)

    # Verify enabled fields in metadata
    metadata_path = S3Helper.generate_remote_path(S3Helper.get_metadata_folder('metadata/Event'), remote_testing_folder) + IntegrationHelper.time_path
    S3Helper.download_files(metadata_path, 'tmp/Event/')
    metadata = File.open(Dir['tmp/Event/*_metadata.json'].sort_by { |file| file.split('/')[-1].split('_')[0].to_i}.last).read
    expect(metadata).to eq expected_enabled_fields_metadata
    GoodData::Connectors::DownloaderCsv::S3Helper.clear_bucket(remote_testing_folder)
  end

  it 'prepares data for PGP encryption in global setting' do
    test_pgp_encryption('spec/data/configurations/s3/configuration_12.json')
  end

  it 'prepares data for PGP encryption in entities setting' do
    test_pgp_encryption('spec/data/configurations/s3/configuration_13.json')
  end

  it 'prepares data for PGP encryption in entities setting with encrypted meta' do
    test_pgp_encryption('spec/data/configurations/s3/configuration_15.json', true)
  end

  it 'prepares data for PGP encryption in entities setting with encrypted meta and generate meta' do
    test_pgp_encryption('spec/data/configurations/s3/configuration_16.json', true, true)
  end

  it 'prepares data for encoding field' do
    remote_testing_folder = S3Helper.generate_testing_folder_name
    con_params = S3Helper.build_connection_params(remote_testing_folder)
    replacement = {
        '$REMOTE_TESTING_FOLDER' => remote_testing_folder
    }

    expected_event_metadata = IntegrationHelper.loadMetadataFile('spec/data/files/feature/expected_metadata_11.json')
    # Prepare data download from CSV
    remote_config_path = S3Helper.generate_remote_path('configuration.json', remote_testing_folder)
    S3Helper.upload_file('spec/data/configurations/s3/configuration_11.json', remote_config_path, replacement)
    remote_data_path = S3Helper.generate_remote_path('data_files/events.gz', remote_testing_folder)
    S3Helper.upload_file('spec/data/files/feature/events0.gz', remote_data_path)
    manifest_path = S3Helper.generate_remote_path('manifests/manifest_1.20160714105300.csv', remote_testing_folder)
    S3Helper.upload_file('spec/data/files/feature/manifest_4.20160714105300.csv', manifest_path, replacement)
    feed_path = S3Helper.generate_remote_path('feed', remote_testing_folder)
    S3Helper.upload_file('spec/data/files/feature/feed7', feed_path)

    metadata = Metadata.new(con_params)
    downloader = Csv.new(metadata, con_params)
    downloader.execute(con_params)

    # Verify disabled fields in metadata
    metadata_path = S3Helper.generate_remote_path(S3Helper.get_metadata_folder('metadata/Event'), remote_testing_folder) + IntegrationHelper.time_path
    S3Helper.download_files(metadata_path, 'tmp/Event/')
    metadata = File.open(Dir['tmp/Event/*_metadata.json'].sort_by { |file| file.split('/')[-1].split('_')[0].to_i}.last).read
    expect(metadata).to eq expected_event_metadata
    GoodData::Connectors::DownloaderCsv::S3Helper.clear_bucket(remote_testing_folder)
  end

  it 'prepares data for ads and process delete and move data' do
    remote_testing_folder = S3Helper.generate_testing_folder_name
    con_params = S3Helper.build_connection_params(remote_testing_folder)
    replacement = {
        '$REMOTE_TESTING_FOLDER' => remote_testing_folder
    }
    expected_metadata = IntegrationHelper.loadMetadataFile('spec/data/files/feature/expected_metadata_1.json')

    remote_config_path = S3Helper.generate_remote_path('configuration.json', remote_testing_folder)
    S3Helper.upload_file('spec/data/configurations/s3/configuration_14.json', remote_config_path, replacement)
    remote_data_folder = S3Helper.generate_remote_path('data_files/', remote_testing_folder)
    remote_data_path = remote_data_folder + 'event_1.0.csv'
    S3Helper.upload_file('spec/data/files/feature/events0.csv', remote_data_path)

    metadata = Metadata.new(con_params)
    downloader = Csv.new(metadata, con_params)
    downloader.execute(con_params)

    metadata_path = S3Helper.generate_remote_path(S3Helper.get_metadata_folder('metadata/Event'), remote_testing_folder) + IntegrationHelper.time_path
    S3Helper.download_files(metadata_path, 'tmp/Event/')
    files_path = S3Helper.generate_remote_path(S3Helper.get_data_folder('Event'), remote_testing_folder) + IntegrationHelper.time_path
    S3Helper.download_files(files_path, 'tmp/Event/')

    data = File.open(Dir["tmp/Event/*_data_#{@padding}.csv"].first).read
    metadata = File.open(Dir['tmp/Event/*_metadata.json'].first).read

    expect(data).to eq File.open('spec/data/files/feature/events0.csv').read
    expect(metadata).to eq expected_metadata

    move_files = S3Helper.list_files(remote_data_folder + 'data_processed')
    expect(move_files.length).to eq 1
    move_files.each do |file|
      expect(file.include?('event_1.0.csv')).to eq true
    end

    GoodData::Connectors::DownloaderCsv::S3Helper.clear_bucket(remote_testing_folder)
  end

  it 'prepares data for custom field in entity' do
    remote_testing_folder = S3Helper.generate_testing_folder_name
    con_params = S3Helper.build_connection_params(remote_testing_folder)
    replacement = {
        '$REMOTE_TESTING_FOLDER' => remote_testing_folder
    }

    expected_event_metadata = IntegrationHelper.loadMetadataFile('spec/data/files/feature/expected_metadata_12.json')
    # Prepare data download from CSV
    remote_config_path = S3Helper.generate_remote_path('configuration.json', remote_testing_folder)
    S3Helper.upload_file('spec/data/configurations/s3/configuration_17.json', remote_config_path, replacement)
    remote_data_path = S3Helper.generate_remote_path('data_files/events.gz', remote_testing_folder)
    S3Helper.upload_file('spec/data/files/feature/events0.gz', remote_data_path)
    manifest_path = S3Helper.generate_remote_path('manifests/manifest_1.20160714105300.csv', remote_testing_folder)
    S3Helper.upload_file('spec/data/files/feature/manifest_4.20160714105300.csv', manifest_path, replacement)
    feed_path = S3Helper.generate_remote_path('feed', remote_testing_folder)
    S3Helper.upload_file('spec/data/files/feature/feed7', feed_path)

    metadata = Metadata.new(con_params)
    downloader = Csv.new(metadata, con_params)
    downloader.execute(con_params)

    # Verify disabled fields in metadata
    metadata_path = S3Helper.generate_remote_path(S3Helper.get_metadata_folder('metadata/Event'), remote_testing_folder) + IntegrationHelper.time_path
    S3Helper.download_files(metadata_path, 'tmp/Event/')
    metadata = File.open(Dir['tmp/Event/*_metadata.json'].sort_by { |file| file.split('/')[-1].split('_')[0].to_i}.last).read
    expect(metadata).to eq expected_event_metadata
    GoodData::Connectors::DownloaderCsv::S3Helper.clear_bucket(remote_testing_folder)
  end

  it 'prepares data from xlsx file in settings downloader without manifests with feed file (City full)' do
    remote_testing_folder = S3Helper.generate_testing_folder_name
    con_params = S3Helper.build_connection_params(remote_testing_folder)
    replacement = {
        '$REMOTE_TESTING_FOLDER' => remote_testing_folder
    }
    expected_batch_pattern = %r(\{\n\ \ "identification":\ "csv_downloader_1",\n\ \ "sequence":\ null,\n\ \ "filename":\ "manifest\-#{Time.now.strftime("%Y") + Time.now.strftime("%m") + Time.now.strftime("%d")}\d{6}",\n\ \ "files":\ \[\n\ \ \ \ \{\n\ \ \ \ \ \ "entity":\ "City",\n\ \ \ \ \ \ "file":\ "ETL_tests\/connectors_downloader_csv\/#{remote_testing_folder}\/#{ConnectionHelper::DEFAULT_DOWNLOADER}\/City\/#{Regexp.quote(IntegrationHelper.time_path)}\/\d{16}_data_#{@padding}\.csv"\n\ \ \ \ \},\n\ \ \ \ \{\n\ \ \ \ \ \ "entity":\ "Zombie",\n\ \ \ \ \ \ "file":\ "ETL_tests\/connectors_downloader_csv\/#{remote_testing_folder}\/#{ConnectionHelper::DEFAULT_DOWNLOADER}\/Zombie\/#{Regexp.quote(IntegrationHelper.time_path)}\/\d{16}_data_#{@padding}\.csv"\n\ \ \ \ \}\n\ \ \]\n\})
    expected_batch_pattern2 = %r(\{\n\ \ "identification":\ "csv_downloader_1",\n\ \ "sequence":\ null,\n\ \ "filename":\ "manifest\-#{Time.now.strftime("%Y") + Time.now.strftime("%m") + Time.now.strftime("%d")}\d{6}",\n\ \ "files":\ \[\n\ \ \ \ \{\n\ \ \ \ \ \ "entity":\ "Zombie",\n\ \ \ \ \ \ "file":\ "ETL_tests\/connectors_downloader_csv\/#{remote_testing_folder}\/#{ConnectionHelper::DEFAULT_DOWNLOADER}\/Zombie\/#{Regexp.quote(IntegrationHelper.time_path)}\/\d{16}_data_#{@padding}\.csv"\n\ \ \ \ \},\n\ \ \ \ \{\n\ \ \ \ \ \ "entity":\ "City",\n\ \ \ \ \ \ "file":\ "ETL_tests\/connectors_downloader_csv\/#{remote_testing_folder}\/#{ConnectionHelper::DEFAULT_DOWNLOADER}\/City\/#{Regexp.quote(IntegrationHelper.time_path)}\/\d{16}_data_#{@padding}\.csv"\n\ \ \ \ \}\n\ \ \]\n\})
    expected_metadata = IntegrationHelper.loadMetadataFile('spec/data/files/feature/expected_metadata_3.json')
    expected_runtime_metadata = %r(\{"global"=>"\{\\"value\\":\{\\"start_date\\":null,\\"downloader_id\\":\\"#{ConnectionHelper::DEFAULT_DOWNLOADER}\\",\\"metadata_file\\":\\"\d{16}_metadata\.json\\",\\"metadata_date\\":\{\\"year\\":\\"#{Time.now.strftime('%Y')}\\",\\"month\\":\\"#{Time.now.strftime('%m')}\\",\\"day\\":\\"#{Time.now.strftime('%d')}\\",\\"timestamp\\":\\"\d{16}\\"\},\\"manifest_timestamp\\":\\"\d{10}\\",\\"manifest_version\\":\\"default\\",\\"downloader_version\\":\\"#{GoodData::Connectors::DownloaderCsv::VERSION}\\",\\"num_rows\\":\\"\\",\\"hash\\":\\"unknown\\",\\"batch\\":\\"\d{16}__batch\.json\\",\\"full\\":true,\\"target_ads\\":\\"\\",\\"md5\\":\\"unknown\\",\\"target_predicate\\":null,\\"export_type\\":\\"full\\",\\"source_filename\\":\\"source\/\d{10}_City\.csv\\",\\"original_filename\\":\\"\d{10}_City\.csv\\"\}\}"\})

    runtime_metadata_options = JSON.parse(DEFAULT_RUNTIME_METADATA_OPTIONS.to_json)
    runtime_metadata_options['eq_options']['num_rows'] = ''
    runtime_metadata_options['eq_options']['full'] = true
    runtime_metadata_options['match_options']['original_filename'] = %r{\d{10}_City.csv}
    runtime_metadata_options['match_options']['source_filename'] = %r{source\/\d{10}_City.csv}
    runtime_metadata_validator = RuntimeMetadataValidator.new(runtime_metadata_options)

    remote_config_path = S3Helper.generate_remote_path('configuration.json', remote_testing_folder)
    S3Helper.upload_file('spec/data/configurations/s3/configuration_18.json', remote_config_path, replacement)
    remote_data_folder = S3Helper.generate_remote_path('data_files/', remote_testing_folder)
    remote_data_path = remote_data_folder + 'entities.xlsx'
    S3Helper.upload_file('spec/data/files/data_folder/entities.xlsx', remote_data_path)
    feed_path = S3Helper.generate_remote_path('feed', remote_testing_folder)
    S3Helper.upload_file('spec/data/files/feature/feed2', feed_path)

    metadata = Metadata.new(con_params)
    downloader = Csv.new(metadata, con_params)
    metadata.set_source_context(GoodData::Connectors::DownloaderCsv::ConnectionHelper::DEFAULT_DOWNLOADER, {}, downloader)
    options = metadata.get_configuration_by_type_and_key('csv', 'options');
    options['access_key'] = GoodData::Connectors::DownloaderCsv::ConnectionHelper::DEFAULT_BDS_ACCESS_KEY
    options['secret_key'] = GoodData::Connectors::DownloaderCsv::ConnectionHelper::DEFAULT_BDS_SECRET_KEY
    options['ignore_columns_check'] = true
    GoodData::Connectors::XlsxToCsvMiddleWare.new.call(con_params.merge('metadata_wrapper'=>metadata)) rescue nil
    con_params[:entities_xlsx] = [remote_data_path]
    downloader.execute(con_params)

    batch_path = S3Helper.generate_remote_path(S3Helper.get_batches_folder('batches'), remote_testing_folder) + IntegrationHelper.time_path
    S3Helper.download_files(batch_path, 'tmp/City/')
    # We compare only data and metadata for first entity
    metadata_path = S3Helper.generate_remote_path(S3Helper.get_metadata_folder('metadata/City'), remote_testing_folder) + IntegrationHelper.time_path
    S3Helper.download_files(metadata_path, 'tmp/City/')
    files_path = S3Helper.generate_remote_path(S3Helper.get_data_folder('City'), remote_testing_folder) + IntegrationHelper.time_path
    S3Helper.download_files(files_path, 'tmp/City/')

    batch = File.read(Dir['tmp/City/*_batch.json'].first)
    data = File.open(Dir["tmp/City/*_data_#{@padding}.csv"].sort_by { |file| file.split('/')[-1].split('_')[0].to_i}.first)
    metadata = File.read(Dir['tmp/City/*_metadata.json'].sort_by { |file| file.split('/')[-1].split('_')[0].to_i}.last)

    #batch_validator = BatchValidator.new('csv_downloader_1', nil, %r(manifest\-#{Time.now.strftime("%Y") + Time.now.strftime("%m") + Time.now.strftime("%d")}\d{6}), {'City' => 1, 'Zombie' => 1})
    #expect(batch_validator.validate(batch)).to eq true
    expect(S3Helper.get_metadata(batch, 'City')).to match expected_runtime_metadata
    expect(runtime_metadata_validator.validate(get_metadata(batch, 'City'))).to eq true
    # Batch may have random order of entities, so rather than overcomplicating regexp we match data against both our expected patterns
    expect(batch.match(expected_batch_pattern) || batch.match(expected_batch_pattern2)).to be_truthy

    move_files = S3Helper.list_files(remote_data_folder + 'data_processed')
    expect(move_files.length).to eq 3
    move_files.each do |file|
      expect(file.include?('City') || file.include?('zombie') || file.include?('entities.xlsx')).to eq true
    end
  end

  it 'prepares data in setting downloader for ads and process delete and move data' do
    remote_testing_folder = S3Helper.generate_testing_folder_name
    con_params = S3Helper.build_connection_params(remote_testing_folder)
    replacement = {
        '$REMOTE_TESTING_FOLDER' => remote_testing_folder
    }
    expected_metadata = IntegrationHelper.loadMetadataFile('spec/data/files/feature/expected_metadata_1.json')

    remote_config_path = S3Helper.generate_remote_path('configuration.json', remote_testing_folder)
    S3Helper.upload_file('spec/data/configurations/s3/configuration_19.json', remote_config_path, replacement)
    remote_data_folder = S3Helper.generate_remote_path('data_files/', remote_testing_folder)
    remote_data_path = remote_data_folder + 'event_1.0.csv'
    S3Helper.upload_file('spec/data/files/feature/events0.csv', remote_data_path)

    metadata = Metadata.new(con_params)
    downloader = Csv.new(metadata, con_params)
    metadata.set_source_context(GoodData::Connectors::DownloaderCsv::ConnectionHelper::DEFAULT_DOWNLOADER, {}, downloader)
    options = metadata.get_configuration_by_type_and_key('csv', 'options');
    options['access_key'] = GoodData::Connectors::DownloaderCsv::ConnectionHelper::DEFAULT_BDS_ACCESS_KEY
    options['secret_key'] = GoodData::Connectors::DownloaderCsv::ConnectionHelper::DEFAULT_BDS_SECRET_KEY
    downloader.execute(con_params)

    metadata_path = S3Helper.generate_remote_path(S3Helper.get_metadata_folder('metadata/Event'), remote_testing_folder) + IntegrationHelper.time_path
    S3Helper.download_files(metadata_path, 'tmp/Event/')
    files_path = S3Helper.generate_remote_path(S3Helper.get_data_folder('Event'), remote_testing_folder) + IntegrationHelper.time_path
    S3Helper.download_files(files_path, 'tmp/Event/')

    data = File.open(Dir["tmp/Event/*_data_#{@padding}.csv"].first).read
    metadata = File.open(Dir['tmp/Event/*_metadata.json'].first).read

    expect(data).to eq File.open('spec/data/files/feature/events0.csv').read
    expect(metadata).to eq expected_metadata

    move_files = S3Helper.list_files(remote_data_folder + 'data_processed')
    expect(move_files.length).to eq 1
    move_files.each do |file|
      expect(file.include?('event_1.0.csv')).to eq true
    end

    GoodData::Connectors::DownloaderCsv::S3Helper.clear_bucket(remote_testing_folder)
  end

  def test_pgp_encryption(config_file, encrypted_meta = false, generate_meta = false)
    remote_testing_folder = encrypted_meta ? 'encrypted_data' : S3Helper.generate_testing_folder_name
    con_params = S3Helper.build_connection_params(remote_testing_folder)
    replacement = {
        '$REMOTE_TESTING_FOLDER' => remote_testing_folder
    }

    remote_config_path = S3Helper.generate_remote_path('configuration.json', remote_testing_folder)
    S3Helper.upload_file(config_file, remote_config_path, replacement)
    remote_data_path = S3Helper.generate_remote_path('data_files/Event_1.0_20190302.csv.pgp', remote_testing_folder)
    S3Helper.upload_file('spec/data/files/feature/Event_1.0_20190302.csv.pgp', remote_data_path)
    if !generate_meta
      if encrypted_meta
        manifest_path = S3Helper.generate_remote_path('manifests/manifest_20170714105300.csv.pgp', remote_testing_folder)
        S3Helper.upload_file('spec/data/files/feature/manifest_20170714105300.csv.pgp', manifest_path)
        feed_path = S3Helper.generate_remote_path('feed.pgp', remote_testing_folder)
        S3Helper.upload_file('spec/data/files/feature/feed6.pgp', feed_path)
      else
        manifest_path = S3Helper.generate_remote_path('manifests/manifest_20170714105300.csv', remote_testing_folder)
        S3Helper.upload_file('spec/data/files/feature/manifest_20170714105300.csv', manifest_path, replacement)
        feed_path = S3Helper.generate_remote_path('feed', remote_testing_folder)
        S3Helper.upload_file('spec/data/files/feature/feed6', feed_path)
      end
    end

    encrypted_secret_key = File.open('spec/data/grest-sec-encrypted.asc').read
    secret_key = GoodData::Helpers.decrypt(encrypted_secret_key, ENV['GD_SPEC_PASSWORD'] || ENV['BIA_ENCRYPTION_KEY'])
    params = con_params.merge('pgp_passphrase' => ConnectionHelper::PGP_PASSPHRASE, 'pgp_encryption|private_key' => secret_key)
    params.merge!('pgp_encryption_meta|private_key' => secret_key) if encrypted_meta
    metadata = Metadata.new(params)
    downloader = Csv.new(metadata, params)
    downloader.execute(params)

    files_path = S3Helper.generate_remote_path(S3Helper.get_data_folder('Event'), remote_testing_folder) + IntegrationHelper.time_path
    puts "files_path: #{files_path}"
    S3Helper.download_files(files_path, 'tmp/Event/')

    data = File.open(Dir["tmp/Event/*_data_#{@padding}*"].first).read

    expect(data).to eq File.open('spec/data/files/feature/Event_1.0_20190302.csv').read
  ensure
    GoodData::Connectors::DownloaderCsv::S3Helper.clear_bucket(remote_testing_folder)
    if encrypted_meta
      remote_path = S3Helper.generate_token_remote_path(remote_testing_folder)
      files = S3Helper.list_files(remote_path)
      while (files.size > 0)
        puts "Deleting testing folder: path=#{remote_path} remaining=#{files.size}"
        sleep 1
        files = S3Helper.list_files(remote_path)
      end
    end
  end

  def get_metadata(batch, entity = '')
    parsed_batch = JSON.parse(batch)
    s3object = S3Helper.get_object(parsed_batch['files'].select { |file| file['entity'].match entity.to_s }.first['file'])
    s3object.metadata.to_h.to_hash
  end
end
