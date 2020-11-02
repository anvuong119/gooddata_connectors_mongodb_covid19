require 'mongo'

Mongo::Logger.logger       = Logger.new('mongo.log')
Mongo::Logger.logger.level = Logger::INFO

def getDocumentString(document)
    begin
        coordinates = document[:loc][:coordinates]
        docStr = document[:country_iso3] + ','
        docStr += document[:country] + ','
        docStr += coordinates[0].to_s + ';' + coordinates[1].to_s + ','
        docStr += document[:population].to_s + ','
        docStr += document[:confirmed].to_s + ','
        docStr += document[:deaths].to_s + ','
        docStr += document[:recovered].to_s + ','
        docStr += document[:date].strftime("%m/%d/%Y")
        return docStr
    rescue StandardError => e
        # puts 'Error occurred in getDocumentString (document: ' + document.to_s + '): ' + e.to_s
        return ''
    end
end

def writeDataToFile(client)
    begin
        File.open('covid19.csv', 'w') do |file|
            file.puts 'code,country,latlon,population,confirmed,deaths,recovered,date'
            client[:global].find.each do |document|
                docStr = getDocumentString(document)
                if not docStr.empty?
                    file.puts docStr
                end
            end
        end
    rescue StandardError => e
        puts 'Error occurred while writing data to file: ' + e.to_s
    end
end

def uploadToS3(filename)
    begin
        S3Helper = GoodData::Connectors::DownloaderCsv::S3Helper
        S3Helper.upload_file(filename, S3Helper.generate_remote_path(filename))
    rescue StandardError => e
        puts 'Error occurred while uploading csv file to S3: ' + e.to_s
    end
end

begin
    puts 'Connecting to server...'
    client = Mongo::Client.new('mongodb+srv://readonly:readonly@covid-19.hip2i.mongodb.net/covid19')

    puts 'Connected to server, fetching data...'
    writeDataToFile(client)

    client.close
    puts 'Fetching data completed.'

    puts 'Uploading csv file to S3...'
    uploadToS3('covid19.csv')
    puts 'Uploading csv file to S3 completed.'
rescue Mongo::Error::NoServerAvailable => e
    puts 'Could not connect to server: ' + e.to_s
end