require 'mongo'

Mongo::Logger.logger       = Logger.new('mongo.log')
Mongo::Logger.logger.level = Logger::INFO

client = Mongo::Client.new('mongodb+srv://readonly:readonly@covid-19.hip2i.mongodb.net/covid19')
db = client.database

puts 'collections: '
puts db.collections # returns a list of collection objects

puts 'collectionNames: '
puts db.collection_names # returns a list of collection names