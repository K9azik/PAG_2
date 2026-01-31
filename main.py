import geopandas as gpd
import pandas as pd
from databases import *
from astral import LocationInfo
from astral.sun import sun

def prepare_data(stations_path, boundary_path):

    stations = gpd.read_file(stations_path)
    boundaries = gpd.read_file(boundary_path)

    #wszystko do wgs84 bo astral inaczej nie zadziala - potem zmienic do wizualizacji na 2180
    stations = stations.to_crs(epsg=4326)
    boundaries = boundaries.to_crs(epsg=4326)
    # if stations.crs != boundaries.crs:
    #     boundaries = boundaries.to_crs(stations.crs)

    counties_json = json.loads(boundaries.to_json(default=str))['features']

    boundaries_subset = boundaries[['geometry', 'name', 'id']].rename(
        columns={'name': 'nazwa_powiatu', 'id': 'id_powiatu'})

    joined = gpd.sjoin(stations, boundaries_subset, how='left', predicate='within')
    stations_json = json.loads(joined.to_json(default=str))['features']

    for feature in stations_json:
        props = feature['properties']
        props['powiatinfo'] = {
            'nazwa': props.get('nazwa_powiatu'),
            'id': props.get('id_powiatu')}
        props.pop('nazwa_powiatu', None)
        props.pop('id_powiatu', None)
        props.pop('index_right', None)

    return stations_json, counties_json

def prepare_csv(csv_path):
    df = pd.read_csv(csv_path, sep=';', names=['station_id', 'm_type', 'date', 'values'])

    df['date'] = pd.to_datetime(df['date'])
    df['date_day'] = df['date'].dt.date.astype(str)
    df['date_time'] = df['date'].dt.strftime('%H:%M')

    docs = []

    for (s_id, m_t, d_day), group in df.groupby(['station_id', 'm_type', 'date_day']):
        measurements = []
        for _, row in group.iterrows():
            measurements.append({
                'time': str(row['date_time']),
                'value': float(row['values'])
            })

        doc = {
            'station_id': int(s_id),
            'm_type': str(m_t),
            'date': str(d_day),
            'values': measurements
        }

        docs.append(doc)
    return docs

class AnalysisManager:
    def __init__(self, mongo_mgr, redis_mgr):
        self.mongo = mongo_mgr
        self.redis = redis_mgr

    def prepare_dataframe(self):
        measurements = list(self.mongo.db.stacje.find({}))

        if not measurements:
            print('Analysis // No data found.')
            return pd.DataFrame()

        daytime = []
        for doc in measurements:
            s_id = str(doc['station_id'])
            date = str(doc['date'])

            position = self.redis.db.geopos('station_points', s_id)
            if not position or position[0] is None:
                continue

            ## ASTRAl ##
            lon, lat = position[0]
            loc = LocationInfo(latitude=lat, longitude=lon)
            s = sun(loc.observer, date=pd.to_datetime(date))
            sunrise = s['sunrise'].strftime('%H:%M')
            sunset = s['sunset'].strftime('%H:%M')

            for m in doc['values']:
                m_time = m['time']

                is_day = sunrise <= m_time <= sunset

                daytime.append({
                    'station_id': int(s_id),
                    'date': date,
                    'time': m_time,
                    'is_day': True if is_day else False,
                    'value': m['value']
                })

        return pd.DataFrame(daytime)


def main(stations_path, measurement_path, boundary_path):
    m = MongoManager()
    r = RedisManager()

    mongo_empty = m.db.stacje.count_documents({}) == 0 or m.db.powiaty.count_documents({}) == 0
    redis_empty = not r.db.exists('station_points')

    if mongo_empty or redis_empty:
        measurement_data = prepare_csv(measurement_path)
        station_data, county_data = prepare_data(stations_path, boundary_path)

        if mongo_empty:
            m.insert_data(measurement_data, county_data)

        if redis_empty:
            r.insert_data(station_data)

    a = AnalysisManager(m,r)
    df = a.prepare_dataframe()
    print(df)

if __name__ == "__main__":
    main(r'Dane/effacility.geojson', r'Dane/B00300S_2025_09.csv', r'Dane/powiaty.shp')





