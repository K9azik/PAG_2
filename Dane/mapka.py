import folium

def map_creator(analysis_mgr):
    counties_data = list(analysis_mgr.mongo.db.powiaty.find({}, {'_id': 0}))

    m = folium.Map(location=[52.0, 19.0], zoom_start=6)

    if counties_data:
        folium.GeoJson(
            {'type': 'FeatureCollection', 'features': counties_data},
            style_function=lambda x: {'fillColor': 'green', 'color': 'black', 'weight': 1, 'fillOpacity': 0.1},
            name="Powiaty").add_to(m)

    stations_in_redis = analysis_mgr.redis.db.zrange('station_points', 0, -1)

    for s_id in stations_in_redis:
        s_id_str = s_id.decode('utf-8') if isinstance(s_id, bytes) else str(s_id)
        pos = analysis_mgr.redis.db.geopos('station_points', s_id_str)

        if pos and pos[0]:
            lon, lat = pos[0]
            folium.CircleMarker(
                location=[lat, lon],
                radius=5,
                color='red',
                fill=True,
                popup=f'Stacja ID: {s_id_str}').add_to(m)

    m.save('Wizualizacja.html')