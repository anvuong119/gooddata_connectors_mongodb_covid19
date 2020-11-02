require 'gdc_etl_csv_downloader'

describe GoodData::Connectors::DownloaderCsv::CsvDownloaderMiddleWare do
  it 'Is defined' do
    expect(GoodData::Connectors::DownloaderCsv::CsvDownloaderMiddleWare).to be_truthy
  end
end
