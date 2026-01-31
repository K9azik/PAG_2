import pymongo
import redis
import json

class MongoManager:
    def __init__(self, host="mongodb://localhost:27017/", database='projekt2'):
       try:
           self.client = pymongo.MongoClient()
           self.db = self.client[database]

           print('Connected to MongoDB.')
       except Exception as e:
           print(f'Failed to connect to MongoDB: {e}.')

    def insert_data(self, stations, counties):
        if not stations:
            print('Mongo // No stations to insert.')
        elif not counties:
            print('Mongo // No counties to insert.')

        try:
            s_collection = self.db.stacje
            s_collection.delete_many({})
            s_collection.insert_many(stations)
            print('Mongo // Stations data inserted successfully.')

            c_collection = self.db.powiaty
            c_collection.delete_many({})
            c_collection.insert_many(counties)
            print('Mongo // Counties data inserted successfully.')

        except Exception as e:
            print(f'Mongo // Failed to insert data: {e}.')

class RedisManager:
    def __init__(self, host='localhost', port=6379):
        try:
            self.pool = redis.ConnectionPool(host=host, port=port, db=0, decode_responses=True)
            self.db = redis.Redis(connection_pool=self.pool)
            self.db.config_set('stop-writes-on-bgsave-error', 'no')
            print('Connected to Redis.')
        except Exception as e:
            print(f'Failed to connect to Redis: {e}.')

    def insert_data(self, stations):
        if not stations:
            print('Redis // No data to insert.')

        try:
            for s in stations:
                props = s.get('properties', {})
                geom = s.get('geometry', {})
                s_id = props.get('ifcid')

                if s_id:
                    lon, lat = geom['coordinates']
                    station_json = json.dumps(s)
                    self.db.set(f'station:{s_id}', station_json)
                    self.db.geoadd('station_points', (lon, lat, s_id))


            print('Redis // Measurements data inserted successfully.')

        except Exception as e:
            print(f'Redis // Failed to insert data: {e}.')