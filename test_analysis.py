from databases import *
import json

m = MongoManager()
r = RedisManager()

# Analiza powiatu pszczyńskiego od 1 do 10 września
result = analyze_county_day_night(m, r, 'tarnogórski', '2025-09-01', '2025-09-10')

if result:
    print(f"=== POWIAT: {result['county']['name']} ===")
    print(f"Przedział: {result['date_range']['start']} - {result['date_range']['end']}")
    print(f"Liczba stacji: {len(result['stations'])}\n")
    
    for station in result['stations']:
        print(f"Stacja: {station['name']} (ID: {station['station_id']})")
        print(f"  Lokalizacja: {station['geometry']['lon']:.4f}, {station['geometry']['lat']:.4f}")
        print(f"  Powiat wewnętrzny: {station['properties']['powiatinfo']['nazwa']}")
        
        analysis = station['analysis']
        if analysis['avg_temp_day'] is not None:
            print(f"  Średnia temp. DZIEŃ: {analysis['avg_temp_day']:.2f}°C ({analysis['day_measurements']} pomiarów)")
        if analysis['avg_temp_night'] is not None:
            print(f"  Średnia temp. NOC: {analysis['avg_temp_night']:.2f}°C ({analysis['night_measurements']} pomiarów)")
        
        if analysis['avg_temp_day'] and analysis['avg_temp_night']:
            diff = analysis['avg_temp_day'] - analysis['avg_temp_night']
            print(f"  Różnica dzień-noc: {diff:.2f}°C")
        print()
    
    # Podsumowanie dla całego powiatu
    all_day_temps = [s['analysis']['avg_temp_day'] for s in result['stations'] if s['analysis']['avg_temp_day']]
    all_night_temps = [s['analysis']['avg_temp_night'] for s in result['stations'] if s['analysis']['avg_temp_night']]
    
    if all_day_temps and all_night_temps:
        print("=== PODSUMOWANIE POWIATU ===")
        print(f"Średnia temp. DZIEŃ (wszystkie stacje): {sum(all_day_temps)/len(all_day_temps):.2f}°C")
        print(f"Średnia temp. NOC (wszystkie stacje): {sum(all_night_temps)/len(all_night_temps):.2f}°C")
        print(f"Różnica: {(sum(all_day_temps)/len(all_day_temps)) - (sum(all_night_temps)/len(all_night_temps)):.2f}°C")
