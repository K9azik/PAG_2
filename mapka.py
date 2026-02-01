import folium
from shapely.geometry import shape

def map_creator(analysis_result):
    lats = [s['geometry']['lat'] for s in analysis_result['stations']]
    lons = [s['geometry']['lon'] for s in analysis_result['stations']]
    center = [sum(lats) / len(lats), sum(lons) / len(lons)]
    
    m = folium.Map(location=center, zoom_start=10)

    all_day = [s['analysis']['avg_temp_day'] for s in analysis_result['stations']]
    all_night = [s['analysis']['avg_temp_night'] for s in analysis_result['stations']]
    
    county_geojson = {
        'type': 'Feature',
        'geometry': analysis_result['county_geometry'],
        'properties': analysis_result['county']
    }
    
    avg_day = sum(all_day) / len(all_day)
    avg_night = sum(all_night) / len(all_night)
    
    county_popup_html = f"""
    <div style="font-family: Arial; min-width: 250px; padding: 5px">
        <h3 style="margin: 0 0 10px 0; color: #2c5aa0; border-bottom: 2px solid #2c5aa0; padding-bottom: 5px">
            {analysis_result['county']['name']}
        </h3>
        <p style="margin: 8px 0; color: #555">
            <b>Okres:</b> {analysis_result['date_range']['start']} - {analysis_result['date_range']['end']}<br>
            <b>Liczba stacji:</b> {len(analysis_result['stations'])}
        </p>
        <h4 style="margin: 10px 0 5px 0; color: #2c5aa0">PODSUMOWANIE</h4>
        <p style="margin: 5px 0; line-height: 1.6">
            <b>Średnia DZIEŃ:</b> {avg_day:.2f}°C <small style="color: #777">(ze {len(all_day)} stacji)</small><br>
            <b>Średnia NOC:</b> {avg_night:.2f}°C <small style="color: #777">(ze {len(all_night)} stacji)</small><br>
            <b>Różnica:</b> <span style="color: #d9534f; font-weight: bold">{avg_day - avg_night:.2f}°C</span>
        </p>
    </div>"""
    
    folium.GeoJson(
        county_geojson,
        popup=folium.Popup(county_popup_html, max_width=300)
    ).add_to(m)
    
    for station in analysis_result['stations']:
        lat, lon = station['geometry']['lat'], station['geometry']['lon']
        analysis = station['analysis']
        diff = analysis['avg_temp_day'] - analysis['avg_temp_night']
        
        popup_html = f"""
        <div style="font-family: Arial; min-width: 250px; padding: 5px">
            <h3 style="margin: 0 0 10px 0; color: #2c5aa0; border-bottom: 2px solid #2c5aa0; padding-bottom: 5px">
                {station['name']}
            </h3>
            <p style="margin: 8px 0; color: #555">
                <b>ID:</b> {station['station_id']}<br>
                <b>Lokalizacja:</b> {lat:.4f}, {lon:.4f}
            </p>
            <h4 style="margin: 10px 0 5px 0; color: #2c5aa0">POMIARY</h4>
            <p style="margin: 5px 0; line-height: 1.6">
                <b>Średnia DZIEŃ:</b> {analysis['avg_temp_day']:.2f}°C <small style="color: #777">({analysis['day_measurements']} pom.)</small><br>
                <b>Średnia NOC:</b> {analysis['avg_temp_night']:.2f}°C <small style="color: #777">({analysis['night_measurements']} pom.)</small><br>
                <b>Różnica:</b> <span style="color: #d9534f; font-weight: bold">{diff:.2f}°C</span>
            </p>
        </div>"""
        
        folium.CircleMarker(
            [lat, lon],
            radius=8,
            color='darkred',
            fill_color='red',
            fill_opacity=0.7,
            popup=folium.Popup(popup_html, max_width=300)
        ).add_to(m)
    
    m.save('Wizualizacja.html')
    print(f"Mapa zapisana: {len(analysis_result['stations'])} stacji w powiecie {analysis_result['county']['name']}")