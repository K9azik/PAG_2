import pymongo
import geopandas as gpd
import json
import redis
import requests

def prepare_data(stations_path, boundary_path):

    stations = gpd.read_file(stations_path)
    boundaries = gpd.read_file(boundary_path)

    #WAZNE: stacje sa w ukladzie 2180 ale metadane z geojsona nie zostaly zapisane w bazie wiec trzeba pamietac
    if stations.crs != boundaries.crs:
        boundaries = boundaries.to_crs(stations.crs)

    boundaries_subset = boundaries[['geometry', 'name', 'id']].rename(
        columns={'name': 'nazwa_powiatu', 'id': 'id_powiatu'})

    joined = gpd.sjoin(stations, boundaries_subset, how="left", predicate="within")
    stations_json = json.loads(joined.to_json(default=str))["features"]

    for feature in stations_json:
        props = feature['properties']
        props['powiatinfo'] = {
            'nazwa': props.get('nazwa_powiatu'),
            'id': props.get('id_powiatu')}
        props.pop('nazwa_powiatu', None)
        props.pop('id_powiatu', None)
        props.pop('index_right', None)

    return stations_json

class MongoManager:
    def __init__(self, host="mongodb://localhost:27017/", database="projekt2"):
       try:
           self.client = pymongo.MongoClient()
           self.db = self.client[database]
           print('Connected to MongoDB.')
       except Exception as e:
           print(f"Failed to connect to MongoDB: {e}.")

    def insert_data(self, stations):
        if not stations:
            print("No data to insert.")
            return

        try:
            collection = self.db.stacje
            collection.delete_many({})

            collection.insert_many(stations)
            print("Data inserted successfully.")

            return list(collection.find())

        except Exception as e:
            print(f"Failed to insert data: {e}.")
            return None

class RedisManager:
    def __init__(self, host="localhost", port=6379):
        try:
            self.r = redis.Redis(host=host, port=port, decode_responses=True)
            self.r.config_set("stop-writes-on-bgsave-error", "no")
            print("Connected to Redis.")
        except Exception as e:
            print(f"Failed to connect to Redis: {e}.")

    def push_tasks(self, data):
        for stat in data:
            props = stat.get("properties")
            task = {
                "ifcid": props.get("ifcid"),
                "url": props.get("gmlidentif")}

            self.r.lpush("queue", json.dumps(task))
        print(f"Pushed {len(data)} tasks to Redis queue.")

    def get_task(self):
        task = self.r.lpop("queue")
        return json.loads(task) if task else None


def process_task(redis_mgr):
    task = redis_mgr.get_task()
    if task is None:
        print("No tasks found.")
        return False

    url = task.get("url")
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(url, timeout=10, headers=headers)
        if response.status_code == 200:
            print("Data fetched successfully")
        else:
            print(f"Request failed for {url} with status: {response.status_code}")

    except Exception as e:
        print(f"Error fetching data: {e}")

    return True

if __name__ == "__main__":
    # 1 geojson, 2 shp powiaty / wojewodztwa
    stations = prepare_data(r"Dane/effacility.geojson", r"Dane/powiaty.shp")

    m = MongoManager()
    db_data = m.insert_data(stations)

    r = RedisManager()
    r.push_tasks(db_data)

    while process_task(r):
        pass

    """
    Nie dzialaja linki z geojsona nie wiem co z tym zrobic przykro mi
    """