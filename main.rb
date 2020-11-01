require 'mongo'

Mongo::Logger.logger       = Logger.new('mongo.log')
Mongo::Logger.logger.level = Logger::INFO

def getDocumentString(document)
    begin
        coordinates = document[:loc].nil? ? nil : document[:loc][:coordinates]
        docStr = document[:country_iso3].nil? ? ',' : document[:country_iso3] + ','
        docStr += document[:country] + ','
        if not coordinates.nil?
            docStr += coordinates[0].to_s + ';' + coordinates[1].to_s + ','
        else
            docStr += ';'
        end
        docStr += document[:population].to_s + ','
        docStr += document[:confirmed].to_s + ','
        docStr += document[:deaths].to_s + ','
        docStr += document[:recovered].to_s + ','
        docStr += document[:date].strftime("%m/%d/%Y")
        return docStr
    rescue StandardError => e
        puts 'Error occurred in getDocumentString (document: ' + document.to_s + '): ' + e.to_s
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

begin
    puts 'Connecting to server...'
    client = Mongo::Client.new('mongodb+srv://readonly:readonly@covid-19.hip2i.mongodb.net/covid19')

    puts 'Connected to server, fetching data...'
    writeDataToFile(client)

    client.close
    puts 'Fetching data completed.'
rescue Mongo::Error::NoServerAvailable => e
    puts 'Could not connect to server: ' + e.to_s
end