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


def get_county_data(mongo_mgr, redis_mgr, county_name, start_date, end_date):
    """Wyciąga dane stacji z powiatu w przedziale dat. Zwraca dict z 'stations' i 'measurements'."""
    county = mongo_mgr.db.powiaty.find_one({'properties.name': county_name})
    if not county:
        return None
    
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
    
    return {'county': county, 'stations': stations, 'measurements': measurements}


def analyze_county_day_night(mongo_mgr, redis_mgr, county_name, start_date, end_date):
    data = get_county_data(mongo_mgr, redis_mgr, county_name, start_date, end_date)
    if not data:
        return None
    
    results = {
        'county': data['county']['properties'],
        'date_range': {'start': start_date, 'end': end_date},
        'stations': []
    }
    
    for station in data['stations']:
        station_id = station['properties']['ifcid']
        coords = station['geometry']['coordinates']
        lon, lat = coords[0], coords[1]
        
        station_measurements = [m for m in data['measurements'] if m['station_id'] == station_id]
        
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