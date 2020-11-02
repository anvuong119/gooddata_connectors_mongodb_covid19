require 'gdc_etl_csv_downloader'

describe GoodData::Connectors::DownloaderCsv::RemoteObject do
  it 'Is defined' do
    expect(GoodData::Connectors::DownloaderCsv::RemoteObject)
    expect(GoodData::Connectors::DownloaderCsv::RemoteObject).to be_kind_of(Class)
  end

  it 'Initialize' do
    remote_object = GoodData::Connectors::DownloaderCsv::RemoteObject.new('some/random/path.txt')
    expect(remote_object.key).to eq('some/random/path.txt')
  end
end
