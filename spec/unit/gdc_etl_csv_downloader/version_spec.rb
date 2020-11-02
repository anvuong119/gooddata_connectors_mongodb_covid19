require 'gdc_etl_csv_downloader'

describe GoodData::Connectors::DownloaderCsv do
  describe 'VERSION' do
    it 'Is defined' do
      expect(GoodData::Connectors::DownloaderCsv::VERSION).to be_kind_of(String)
    end
  end
end
