require 'mongo'

Mongo::Logger.logger       = Logger.new('mongo.log')
Mongo::Logger.logger.level = Logger::INFO

begin
    client = Mongo::Client.new('mongodb+srv://readonly:readonly@covid-19.hip2i.mongodb.net/covid19')
    client[:global].find.each do |document|
        puts document
    end
    client.close
rescue Mongo::Error::NoServerAvailable => e
    puts 'Could not connect to server'
    puts e
end