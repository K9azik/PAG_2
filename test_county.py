from databases import *

m = MongoManager()
r = RedisManager()

# Przykład: Powiat pszczyński (ma dane w bazie)
result = get_county_data(m, r, 'pszczyński', '2025-09-01', '2025-09-10')

if result:
    print(f"Powiat: {result['county']['properties']['name']}")
    print(f"Liczba stacji: {len(result['stations'])}")
    print(f"Liczba pomiarów: {len(result['measurements'])}")
    
    print("\nStacje:")
    for s in result['stations']:
        print(f"  - {s['properties']['name1']} (ID: {s['properties']['ifcid']})")
    
    print("\nPrzykładowy pomiar:")
    if result['measurements']:
        m = result['measurements'][0]
        print(f"  Stacja: {m['station_id']}, Data: {m['date']}, Wartości: {len(m['values'])}")
else:
    print("Nie znaleziono powiatu")
