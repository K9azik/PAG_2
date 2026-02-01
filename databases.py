import pymongo
import redis
import json
import pandas as pd
from astral import LocationInfo
from astral.sun import sun

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
                    self.db.geoadd('station_points', lon, lat, s_id)


            print('Redis // Measurements data inserted successfully.')

        except Exception as e:
            print(f'Redis // Failed to insert data: {e}.')

def get_counties_with_station_count(mongo_mgr, redis_mgr):
    station_ids_with_data = set(mongo_mgr.db.stacje.distinct('station_id'))
    
    counties = list(mongo_mgr.db.powiaty.find({}, {'properties.name': 1, 'properties.id': 1, '_id': 0}))
    
    county_id_to_name = {}
    for county in counties:
        if 'properties' in county:
            county_name = county['properties'].get('name')
            county_id = county['properties'].get('id')
            if county_name and county_id:
                county_id_to_name[county_id] = county_name
    
    county_counts = {name: 0 for name in county_id_to_name.values()}
    
    for key in redis_mgr.db.keys('station:*'):
        station_json = redis_mgr.db.get(key)
        if station_json:
            station = json.loads(station_json)
            s_id = station.get('properties', {}).get('ifcid')
            county_id = station.get('properties', {}).get('powiatinfo', {}).get('id')
            
            if s_id in station_ids_with_data and county_id and county_id in county_id_to_name:
                county_name = county_id_to_name[county_id]
                county_counts[county_name] += 1
    
    return county_counts


def get_date_range(mongo_mgr):
    pipeline = [
        {'$group': {
            '_id': None,
            'min_date': {'$min': '$date'},
            'max_date': {'$max': '$date'}
        }}
    ]
    result = list(mongo_mgr.db.stacje.aggregate(pipeline))
    if result:
        return result[0]['min_date'], result[0]['max_date']
    return None, None


def analyze_county_day_night(mongo_mgr, redis_mgr, county_name, start_date, end_date):
    county = mongo_mgr.db.powiaty.find_one({'properties.name': county_name})
    county_id = county['properties']['id']

    stations, station_ids = [], []
    for key in redis_mgr.db.keys('station:*'):
        station = json.loads(redis_mgr.db.get(key))
        if station['properties'].get('powiatinfo', {}).get('id') == county_id:
            stations.append(station)
            station_ids.append(station['properties']['ifcid'])
    
    measurements = list(mongo_mgr.db.stacje.find({
        'station_id': {'$in': station_ids},
        'date': {'$gte': start_date, '$lte': end_date}
    }))
    
    results = {
        'county': county['properties'],
        'county_geometry': county['geometry'],
        'date_range': {'start': start_date, 'end': end_date},
        'stations': []
    }

    for station in stations:
        station_id = station['properties']['ifcid']
        coords = station['geometry']['coordinates']
        lon, lat = coords[0], coords[1]
        
        station_measurements = [m for m in measurements if m['station_id'] == station_id]
        
        if not station_measurements:
            continue
        
        day_temps, night_temps = [], []
        
        for measurement in station_measurements:
            date = measurement['date']
            
            loc = LocationInfo(latitude=lat, longitude=lon)
            s = sun(loc.observer, date=pd.to_datetime(date))
            sunrise = s['sunrise'].strftime('%H:%M')
            sunset = s['sunset'].strftime('%H:%M')
            
            for v in measurement['values']:
                time = v['time']
                temp = v['value']
                
                if sunrise <= time <= sunset:
                    day_temps.append(temp)
                else:
                    night_temps.append(temp)
        
        station_result = {
            'station_id': station_id,
            'name': station['properties']['name1'],
            'geometry': {'lon': lon, 'lat': lat, 'type': 'Point'},
            'properties': station['properties'],
            'analysis': {
                'avg_temp_day': sum(day_temps) / len(day_temps) if day_temps else None,
                'avg_temp_night': sum(night_temps) / len(night_temps) if night_temps else None,
                'day_measurements': len(day_temps),
                'night_measurements': len(night_temps)
            }
        }
        
        results['stations'].append(station_result)
    
    return results