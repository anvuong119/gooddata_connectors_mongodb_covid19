require 'gdc_etl_csv_downloader'
require 'pry'

describe 'Downloading data from SFTP and preparing metadata, batch and files for ads integrator', type: :feature do
  before :all do
    Csv = GoodData::Connectors::DownloaderCsv::Csv
    Metadata = GoodData::Connectors::Metadata::Metadata
    ConnectionHelper = GoodData::Connectors::DownloaderCsv::ConnectionHelper
    IntegrationHelper = GoodData::Connectors::DownloaderCsv::IntegrationHelper
    S3Helper = GoodData::Connectors::DownloaderCsv::S3Helper
    SftpHelper = GoodData::Connectors::DownloaderCsv::SftpHelper
    REMOTE_TESTING_FOLDER_TEXT = '$REMOTE_TESTING_FOLDER'
    CONFIGURATION_OPTIONS = {
        '$SFTP_HOST' => GoodData::Connectors::DownloaderCsv::ConnectionHelper::SFTP_HOST,
        '$SFTP_USERNAME' => GoodData::Connectors::DownloaderCsv::ConnectionHelper::SFTP_USERNAME,
        '$SFTP_PASSWORD' => GoodData::Connectors::DownloaderCsv::ConnectionHelper::SFTP_PASSWORD
    }

    ROOT_TEST_FOLDER = ConnectionHelper::DEFAULT_BDS_FOLDER.split('/')[0]

    SftpHelper.connect
  end

  after :all do
    SftpHelper.disconnect
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

  it 'prepares data from uncompressed file without manifest and feed files' do
    remote_testing_folder = S3Helper.generate_testing_folder_name
    con_params = S3Helper.build_connection_params(remote_testing_folder)
    replacement = CONFIGURATION_OPTIONS.merge('$REMOTE_TESTING_FOLDER' => remote_testing_folder)
    # upload configuration.json
    remote_config_path = S3Helper.generate_remote_path('configuration.json', remote_testing_folder)
    S3Helper.upload_file('spec/data/configurations/sftp/configuration_1.json', remote_config_path, replacement)

    # prepare metadata on sftp
    remote_data_path = "#{ROOT_TEST_FOLDER}/integration_test/#{remote_testing_folder}/events.csv"
    SftpHelper.upload('spec/data/files/feature/events.csv', remote_data_path)

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

    expect(batch).to match(%r(\{\n\ \ "identification":\ "csv_downloader_1",\n\ \ "sequence":\ null,\n\ \ "filename":\ "manifest\-#{Time.now.strftime("%Y") + Time.now.strftime("%m") + Time.now.strftime("%d")}\d{6}",\n\ \ "files":\ \[\n\ \ \ \ \{\n\ \ \ \ \ \ "entity":\ "Event",\n\ \ \ \ \ \ "file":\ "ETL_tests\/connectors_downloader_csv\/#{remote_testing_folder}\/#{ConnectionHelper::DEFAULT_DOWNLOADER}\/Event\/#{Regexp.quote(IntegrationHelper.time_path)}\/\d{16}_data_#{@padding}\.csv"\n\ \ \ \ \}\n\ \ \]\n\}))
    expect(data).to eq File.open('spec/data/files/feature/events.csv').read
    expect(metadata).to eq IntegrationHelper.loadMetadataFile('spec/data/files/feature/expected_metadata.json')
    S3Helper.clear_bucket(remote_testing_folder)
  end

  it 'prepares data from compressed file with manifests and feed files (x2)' do
    remote_testing_folder = S3Helper.generate_testing_folder_name
    con_params = S3Helper.build_connection_params(remote_testing_folder)
    replacement = CONFIGURATION_OPTIONS.merge('$REMOTE_TESTING_FOLDER' => remote_testing_folder)
    expected_batch_pattern = %r(\{\n\ \ "identification":\ "csv_downloader_1",\n\ \ "sequence":\ 2,\n\ \ "filename":\ "manifest_2\.20160914105300\.csv",\n\ \ "files":\ \[\n\ \ \ \ \{\n\ \ \ \ \ \ "entity":\ "Event",\n\ \ \ \ \ \ "file":\ "ETL_tests\/connectors_downloader_csv\/#{remote_testing_folder}\/#{ConnectionHelper::DEFAULT_DOWNLOADER}\/Event\/#{Regexp.quote(IntegrationHelper.time_path)}\/\d{16}_data_#{@padding}\.gz"\n\ \ \ \ \}\n\ \ \]\n\})
    expected_metadata = IntegrationHelper.loadMetadataFile('spec/data/files/feature/expected_metadata_7.json')

    # prepare configuration
    remote_config_path = S3Helper.generate_remote_path('configuration.json', remote_testing_folder)
    S3Helper.upload_file('spec/data/configurations/sftp/configuration_2.json', remote_config_path, replacement)

    # prepare data
    SftpHelper.upload('spec/data/files/feature/events0.gz', "#{ROOT_TEST_FOLDER}/integration_test/#{remote_testing_folder}/events0.gz")

    # prepare manifest
    SftpHelper.upload('spec/data/manifests/sftp/manifest_1.20160714105300.csv', "#{ROOT_TEST_FOLDER}/integration_test/#{remote_testing_folder}/manifests/manifest_1.20160714105300.csv", replacement)

    # prepare feed
    SftpHelper.upload('spec/data/files/feature/feed', "#{ROOT_TEST_FOLDER}/integration_test/#{remote_testing_folder}/feed")

    metadata = Metadata.new(con_params)
    downloader = Csv.new(metadata, con_params)
    downloader.execute(con_params)

    # prepare next data
    SftpHelper.upload('spec/data/files/feature/events.gz', "#{ROOT_TEST_FOLDER}/integration_test/#{remote_testing_folder}/events.gz")

    # prepare next manifest
    SftpHelper.upload('spec/data/manifests/sftp/manifest_2.20160914105300.csv', "#{ROOT_TEST_FOLDER}/integration_test/#{remote_testing_folder}/manifests/manifest_2.20160914105300.csv", replacement)

    # prepare feed
    SftpHelper.upload('spec/data/files/feature/feed4', "#{ROOT_TEST_FOLDER}/integration_test/#{remote_testing_folder}/feed")

    # second run
    downloader.execute(con_params)

    batch_path = S3Helper.generate_remote_path(S3Helper.get_batches_folder('batches'), remote_testing_folder) + IntegrationHelper.time_path
    S3Helper.download_files(batch_path, 'tmp/Event/')
    metadata_path = S3Helper.generate_remote_path(S3Helper.get_metadata_folder('metadata/Event'), remote_testing_folder) + IntegrationHelper.time_path
    S3Helper.download_files(metadata_path, 'tmp/Event/')
    files_path = S3Helper.generate_remote_path(S3Helper.get_data_folder('Event'), remote_testing_folder) + IntegrationHelper.time_path
    S3Helper.download_files(files_path, 'tmp/Event/')

    batch = File.open(Dir['tmp/Event/*_2_batch.json'].first).read
    data = File.open(Dir["tmp/Event/*_data_#{@padding}.gz"].sort_by { |file| file.split('/')[-1].split('_')[0].to_i}.last)
    metadata = File.open(Dir['tmp/Event/*_metadata.json'].sort_by { |file| file.split('/')[-1].split('_')[0].to_i}.last).read

    expect(batch).to match(expected_batch_pattern)
    expect(FileUtils.compare_file(data, File.open('spec/data/files/feature/events.gz'))).to be true
    expect(metadata).to eq expected_metadata
    S3Helper.clear_bucket(remote_testing_folder)
  end

  it 'prepares data from uncompressed file with manifests and feed files, three runs, first full, other two inc' do
    remote_testing_folder = S3Helper.generate_testing_folder_name
    con_params = S3Helper.build_connection_params(remote_testing_folder)
    replacement = CONFIGURATION_OPTIONS.merge('$REMOTE_TESTING_FOLDER' => remote_testing_folder)
    extra1_padding = 'kXt0ZbFk'
    extra2_padding = 'aXt0AbFz'
    allow(SecureRandom).to receive(:urlsafe_base64).with(6).and_return(@padding,extra1_padding,extra2_padding)
    expected_batch_pattern = %r(\{\n\ \ "identification":\ "csv_downloader_1",\n\ \ "sequence":\ 3,\n\ \ "filename":\ "manifest_3\.20160714105302\.csv",\n\ \ "files":\ \[\n\ \ \ \ \{\n\ \ \ \ \ \ "entity":\ "Event",\n\ \ \ \ \ \ "file":\ "ETL_tests\/connectors_downloader_csv\/#{remote_testing_folder}\/#{ConnectionHelper::DEFAULT_DOWNLOADER}\/Event\/#{Regexp.quote(IntegrationHelper.time_path)}\/\d{16}_data_#{extra2_padding}\.csv"\n\ \ \ \ \}\n\ \ \]\n\})
    expected_metadata = IntegrationHelper.loadMetadataFile('spec/data/files/feature/expected_metadata_5.json')
    expected_runtime_metadata1 = %r(\{"global"=>"\{\\"value\\":\{\\"start_date\\":null,\\"downloader_id\\":\\"#{ConnectionHelper::DEFAULT_DOWNLOADER}\\",\\"metadata_file\\":\\"\d{16}_metadata\.json\\",\\"metadata_date\\":\{\\"year\\":\\"#{Time.now.strftime('%Y')}\\",\\"month\\":\\"#{Time.now.strftime('%m')}\\",\\"day\\":\\"#{Time.now.strftime('%d')}\\",\\"timestamp\\":\\"\d{16}\\"\},\\"manifest_timestamp\\":\\"1468486380\\",\\"manifest_version\\":\\"1\.0\\",\\"downloader_version\\":\\"#{GoodData::Connectors::DownloaderCsv::VERSION}\\",\\"num_rows\\":null,\\"hash\\":\\"unknown\\",\\"batch\\":\\"\d{16}_1_batch\.json\\",\\"full\\":true,\\"sequence\\":1,\\"target_ads\\":null,\\"md5\\":\\"unknown\\",\\"export_type\\":\\"full\\",\\"source_filename\\":\\"source\/events0\.csv\\",\\"original_filename\\":\\"events0\.csv\\"\}\}"\})
    expected_runtime_metadata2 = %r(\{"global"=>"\{\\"value\\":\{\\"start_date\\":null,\\"downloader_id\\":\\"#{ConnectionHelper::DEFAULT_DOWNLOADER}\\",\\"metadata_file\\":\\"\d{16}_metadata\.json\\",\\"metadata_date\\":\{\\"year\\":\\"#{Time.now.strftime('%Y')}\\",\\"month\\":\\"#{Time.now.strftime('%m')}\\",\\"day\\":\\"#{Time.now.strftime('%d')}\\",\\"timestamp\\":\\"\d{16}\\"\},\\"manifest_timestamp\\":\\"1468486380\\",\\"manifest_version\\":\\"1\.0\\",\\"downloader_version\\":\\"#{GoodData::Connectors::DownloaderCsv::VERSION}\\",\\"num_rows\\":null,\\"hash\\":\\"unknown\\",\\"batch\\":\\"\d{16}_2_batch\.json\\",\\"sequence\\":2,\\"target_ads\\":null,\\"md5\\":\\"unknown\\",\\"export_type\\":\\"inc\\",\\"source_filename\\":\\"source\/events\.csv\\",\\"original_filename\\":\\"events\.csv\\"\}\}"\})
    expected_runtime_metadata3 = %r(\{"global"=>"\{\\"value\\":\{\\"start_date\\":null,\\"downloader_id\\":\\"#{ConnectionHelper::DEFAULT_DOWNLOADER}\\",\\"metadata_file\\":\\"\d{16}_metadata\.json\\",\\"metadata_date\\":\{\\"year\\":\\"#{Time.now.strftime('%Y')}\\",\\"month\\":\\"#{Time.now.strftime('%m')}\\",\\"day\\":\\"#{Time.now.strftime('%d')}\\",\\"timestamp\\":\\"\d{16}\\"\},\\"manifest_timestamp\\":\\"1468486380\\",\\"manifest_version\\":\\"1\.0\\",\\"downloader_version\\":\\"#{GoodData::Connectors::DownloaderCsv::VERSION}\\",\\"num_rows\\":null,\\"hash\\":\\"unknown\\",\\"batch\\":\\"\d{16}_3_batch\.json\\",\\"sequence\\":3,\\"target_ads\\":null,\\"md5\\":\\"unknown\\",\\"export_type\\":\\"inc\\",\\"source_filename\\":\\"source\/events1\.csv\\",\\"original_filename\\":\\"events1\.csv\\"\}\}"\})

    # prepare configuration
    remote_config_path = S3Helper.generate_remote_path('configuration.json', remote_testing_folder)
    S3Helper.upload_file('spec/data/configurations/sftp/configuration_5.json', remote_config_path, replacement)
    # prepare 3 data files
    SftpHelper.upload('spec/data/files/feature/events0.csv', "#{ROOT_TEST_FOLDER}/integration_test/#{remote_testing_folder}/events0.csv")
    SftpHelper.upload('spec/data/files/feature/events.csv', "#{ROOT_TEST_FOLDER}/integration_test/#{remote_testing_folder}/events.csv")
    SftpHelper.upload('spec/data/files/feature/events1.csv', "#{ROOT_TEST_FOLDER}/integration_test/#{remote_testing_folder}/events1.csv")
    # prepare 3 manifests
    SftpHelper.upload('spec/data/manifests/sftp/manifest_1.20160714105302.csv', "#{ROOT_TEST_FOLDER}/integration_test/#{remote_testing_folder}/manifests/manifest_1.20160714105302.csv", replacement)
    SftpHelper.upload('spec/data/manifests/sftp/manifest_2.20160714105302.csv', "#{ROOT_TEST_FOLDER}/integration_test/#{remote_testing_folder}/manifests/manifest_2.20160714105302.csv", replacement)
    SftpHelper.upload('spec/data/manifests/sftp/manifest_3.20160714105302.csv', "#{ROOT_TEST_FOLDER}/integration_test/#{remote_testing_folder}/manifests/manifest_3.20160714105302.csv", replacement)
    # prepare feed
    SftpHelper.upload('spec/data/files/feature/feed', "#{ROOT_TEST_FOLDER}/integration_test/#{remote_testing_folder}/feed")

    metadata = Metadata.new(con_params)
    downloader = Csv.new(metadata, con_params)
    downloader.execute(con_params)

    batch_path = S3Helper.generate_remote_path(S3Helper.get_batches_folder('batches'), remote_testing_folder) + IntegrationHelper.time_path
    S3Helper.download_files(batch_path, 'tmp/Event/')
    metadata_path = S3Helper.generate_remote_path(S3Helper.get_metadata_folder('metadata/Event'), remote_testing_folder) + IntegrationHelper.time_path
    S3Helper.download_files(metadata_path, 'tmp/Event/')
    files_path = S3Helper.generate_remote_path(S3Helper.get_data_folder('Event'), remote_testing_folder) + IntegrationHelper.time_path
    S3Helper.download_files(files_path, 'tmp/Event/')

    batch1 = File.open(Dir['tmp/Event/*_1_batch.json'].first).read
    batch2 = File.open(Dir['tmp/Event/*_2_batch.json'].first).read
    batch3 = File.open(Dir['tmp/Event/*_3_batch.json'].first).read
    data = File.open(Dir["tmp/Event/*_data_#{extra2_padding}.csv"].sort_by { |file| file.split('/')[-1].split('_')[0].to_i}.last)
    metadata = File.open(Dir['tmp/Event/*_metadata.json'].sort_by { |file| file.split('/')[-1].split('_')[0].to_i}.last).read

    expect(S3Helper.get_metadata(batch1)).to match expected_runtime_metadata1
    expect(S3Helper.get_metadata(batch2)).to match expected_runtime_metadata2
    expect(S3Helper.get_metadata(batch3)).to match expected_runtime_metadata3

    expect(batch3).to match(expected_batch_pattern)
    expect(FileUtils.compare_file(data, File.open('spec/data/files/feature/events1.csv'))).to be true
    expect(metadata).to eq expected_metadata
    S3Helper.clear_bucket(remote_testing_folder)
  end

  it 'prepares data from xlsx file without manifests with feed file (City full)' do
    remote_testing_folder = S3Helper.generate_testing_folder_name
    con_params = S3Helper.build_connection_params(remote_testing_folder)
    replacement = CONFIGURATION_OPTIONS.merge('$REMOTE_TESTING_FOLDER' => remote_testing_folder)
    expected_batch_pattern = %r(\{\n\ \ "identification":\ "csv_downloader_1",\n\ \ "sequence":\ null,\n\ \ "filename":\ "manifest\-#{Time.now.strftime("%Y") + Time.now.strftime("%m") + Time.now.strftime("%d")}\d{6}",\n\ \ "files":\ \[\n\ \ \ \ \{\n\ \ \ \ \ \ "entity":\ "City",\n\ \ \ \ \ \ "file":\ "ETL_tests\/connectors_downloader_csv\/#{remote_testing_folder}\/#{ConnectionHelper::DEFAULT_DOWNLOADER}\/City\/#{Regexp.quote(IntegrationHelper.time_path)}\/\d{16}_data_#{@padding}\.csv"\n\ \ \ \ \},\n\ \ \ \ \{\n\ \ \ \ \ \ "entity":\ "Zombie",\n\ \ \ \ \ \ "file":\ "ETL_tests\/connectors_downloader_csv\/#{remote_testing_folder}\/#{ConnectionHelper::DEFAULT_DOWNLOADER}\/Zombie\/#{Regexp.quote(IntegrationHelper.time_path)}\/\d{16}_data_#{@padding}\.csv"\n\ \ \ \ \}\n\ \ \]\n\})
    expected_batch_pattern2 = %r(\{\n\ \ "identification":\ "csv_downloader_1",\n\ \ "sequence":\ null,\n\ \ "filename":\ "manifest\-#{Time.now.strftime("%Y") + Time.now.strftime("%m") + Time.now.strftime("%d")}\d{6}",\n\ \ "files":\ \[\n\ \ \ \ \{\n\ \ \ \ \ \ "entity":\ "Zombie",\n\ \ \ \ \ \ "file":\ "ETL_tests\/connectors_downloader_csv\/#{remote_testing_folder}\/#{ConnectionHelper::DEFAULT_DOWNLOADER}\/Zombie\/#{Regexp.quote(IntegrationHelper.time_path)}\/\d{16}_data_#{@padding}\.csv"\n\ \ \ \ \},\n\ \ \ \ \{\n\ \ \ \ \ \ "entity":\ "City",\n\ \ \ \ \ \ "file":\ "ETL_tests\/connectors_downloader_csv\/#{remote_testing_folder}\/#{ConnectionHelper::DEFAULT_DOWNLOADER}\/City\/#{Regexp.quote(IntegrationHelper.time_path)}\/\d{16}_data_#{@padding}\.csv"\n\ \ \ \ \}\n\ \ \]\n\})
    expected_metadata = IntegrationHelper.loadMetadataFile('spec/data/files/feature/expected_metadata_3.json')
    expected_runtime_metadata = %r(\{"global"=>"\{\\"value\\":\{\\"start_date\\":null,\\"downloader_id\\":\\"#{ConnectionHelper::DEFAULT_DOWNLOADER}\\",\\"metadata_file\\":\\"\d{16}_metadata\.json\\",\\"metadata_date\\":\{\\"year\\":\\"#{Time.now.strftime('%Y')}\\",\\"month\\":\\"#{Time.now.strftime('%m')}\\",\\"day\\":\\"#{Time.now.strftime('%d')}\\",\\"timestamp\\":\\"\d{16}\\"\},\\"manifest_timestamp\\":\\"\d{10}\\",\\"manifest_version\\":\\"default\\",\\"downloader_version\\":\\"#{GoodData::Connectors::DownloaderCsv::VERSION}\\",\\"num_rows\\":\\"\\",\\"hash\\":\\"unknown\\",\\"batch\\":\\"\d{16}__batch\.json\\",\\"full\\":true,\\"target_ads\\":\\"\\",\\"md5\\":\\"unknown\\",\\"target_predicate\\":null,\\"export_type\\":\\"full\\",\\"source_filename\\":\\"source\/\d{10}_City\.csv\\",\\"original_filename\\":\\"\d{10}_City\.csv\\"\}\}"\})
    # prepare configuration
    remote_config_path = S3Helper.generate_remote_path('configuration.json', remote_testing_folder)
    S3Helper.upload_file('spec/data/configurations/sftp/configuration_3.json', remote_config_path, replacement)
    # prepare data excel
    SftpHelper.upload('spec/data/files/data_folder/entities.xlsx', "#{ROOT_TEST_FOLDER}/integration_test/#{remote_testing_folder}/entities.xlsx")
    # prepare feed
    SftpHelper.upload('spec/data/files/feature/feed2', "#{ROOT_TEST_FOLDER}/integration_test/#{remote_testing_folder}/feed")

    metadata = Metadata.new(con_params)
    downloader = Csv.new(metadata, con_params)
    GoodData::Connectors::XlsxToCsvMiddleWare.new.call(con_params.merge('metadata_wrapper'=>metadata)) rescue nil
    downloader.execute(con_params)

    batch_path = S3Helper.generate_remote_path(S3Helper.get_batches_folder('batches'), remote_testing_folder) + IntegrationHelper.time_path
    S3Helper.download_files(batch_path, 'tmp/City/')
    metadata_path = S3Helper.generate_remote_path(S3Helper.get_metadata_folder('metadata/City'), remote_testing_folder) + IntegrationHelper.time_path
    S3Helper.download_files(metadata_path, 'tmp/City/')
    files_path = S3Helper.generate_remote_path(S3Helper.get_data_folder('City'), remote_testing_folder) + IntegrationHelper.time_path
    S3Helper.download_files(files_path, 'tmp/City/')

    batch = File.read(Dir['tmp/City/*_batch.json'].first)
    data = File.open(Dir["tmp/City/*_data_#{@padding}.csv"].sort_by { |file| file.split('/')[-1].split('_')[0].to_i}.first)
    metadata = File.read(Dir['tmp/City/*_metadata.json'].sort_by { |file| file.split('/')[-1].split('_')[0].to_i}.last)

    # Batch may have random order of entities, so rather than overcomplicating regexp we match data against both our expected patterns
    expect(batch.match(expected_batch_pattern) || batch.match(expected_batch_pattern2)).to be_truthy
    expect(S3Helper.get_metadata(batch, 'City')).to match expected_runtime_metadata
    expect(FileUtils.compare_file(data, File.open('spec/data/files/data_folder/City_1.0_1468503659.csv'))).to be true
    expect(metadata).to eq expected_metadata
    S3Helper.clear_bucket(remote_testing_folder)
  end

  it 'prepares data from uncompressed file without manifest and feed files with entity_name parsing' do
    remote_testing_folder = S3Helper.generate_testing_folder_name
    con_params = S3Helper.build_connection_params(remote_testing_folder)
    replacement = CONFIGURATION_OPTIONS.merge('$REMOTE_TESTING_FOLDER' => remote_testing_folder)
    expected_batch_pattern = %r(\{\n\ \ "identification":\ "csv_downloader_1",\n\ \ "sequence":\ null,\n\ \ "filename":\ "manifest\-#{Time.now.strftime("%Y") + Time.now.strftime("%m") + Time.now.strftime("%d")}\d{6}",\n\ \ "files":\ \[\n\ \ \ \ \{\n\ \ \ \ \ \ "entity":\ "Event",\n\ \ \ \ \ \ "file":\ "ETL_tests\/connectors_downloader_csv\/#{remote_testing_folder}\/#{ConnectionHelper::DEFAULT_DOWNLOADER}\/Event\/#{Regexp.quote(IntegrationHelper.time_path)}\/\d{16}_data_#{@padding}\.csv"\n\ \ \ \ \}\n\ \ \]\n\})
    expected_metadata = IntegrationHelper.loadMetadataFile('spec/data/files/feature/expected_metadata.json')
    expected_runtime_metadata = %r(\{"global"=>"\{\\"value\\":\{\\"start_date\\":null,\\"downloader_id\\":\\"#{ConnectionHelper::DEFAULT_DOWNLOADER}\\",\\"metadata_file\\":\\"\d{16}_metadata\.json\\",\\"metadata_date\\":\{\\"year\\":\\"#{Time.now.strftime('%Y')}\\",\\"month\\":\\"#{Time.now.strftime('%m')}\\",\\"day\\":\\"#{Time.now.strftime('%d')}\\",\\"timestamp\\":\\"\d{16}\\"\},\\"manifest_timestamp\\":\\"\d{10}\\",\\"manifest_version\\":\\"default\\",\\"downloader_version\\":\\"#{GoodData::Connectors::DownloaderCsv::VERSION}\\",\\"num_rows\\":\\"\\",\\"hash\\":\\"unknown\\",\\"batch\\":\\"\d{16}__batch\.json\\",\\"full\\":true,\\"entity_regex\\":\\"R\.pd\\",\\"target_ads\\":\\"\\",\\"md5\\":\\"unknown\\",\\"target_predicate\\":null,\\"export_type\\":\\"full\\",\\"source_filename\\":\\"source\/Event_20160914_R\.pd\.csv\\",\\"original_filename\\":\\"Event_20160914_R\.pd\.csv\\"\}\}"\})
    # prepare configuration
    remote_config_path = S3Helper.generate_remote_path('configuration.json', remote_testing_folder)
    S3Helper.upload_file('spec/data/configurations/sftp/configuration_6.json', remote_config_path, replacement)
    # prepare data
    SftpHelper.upload('spec/data/files/feature/events.csv', "#{ROOT_TEST_FOLDER}/integration_test/#{remote_testing_folder}/Event_20160914_R.pd.csv")

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
    expect(S3Helper.get_metadata(batch)).to match expected_runtime_metadata
    expect(data).to eq File.open('spec/data/files/feature/events.csv').read
    expect(metadata).to eq expected_metadata
    S3Helper.clear_bucket(remote_testing_folder)
  end
end
