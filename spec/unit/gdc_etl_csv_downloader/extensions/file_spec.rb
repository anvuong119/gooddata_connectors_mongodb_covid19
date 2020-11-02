require 'gdc_etl_csv_downloader'

describe '#zip_to_gzip' do
  it 'Does zip -> gzip' do
    filename = 'spec/data/files/data_folder/dummy/file1.zip'
    filename_gz = 'spec/data/files/data_folder/dummy/file1.gz'
    expect(File.zip_to_gzip(filename)).to eq filename_gz
    File.delete(filename_gz)
  end

  it 'Raises exception if zip contains more than one file' do
    filename = 'spec/data/files/data_folder/dummy/file2.zip'
    expect { File.zip_to_gzip(filename) }.to raise_error(Exception)
  end
end
