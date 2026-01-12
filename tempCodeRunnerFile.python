import geopandas as gpd
import json
from shapely.geometry import shape, mapping

def optimized_geojson(input_path, output_path, type='load'):
    print(f"Reading {input_path}...")
    gdf = gpd.read_file(input_path)
    
    if type == 'load':
        gdf['Peak_34'] = (gdf['FIRST_F2034_Peak_MVA'].astype(float) / 
                          gdf['FIRST_Summer_Rating__MVA_'].astype(float) * 100).round(1)
        gdf['Ready'] = ((gdf['FIRST_F2025_Peak__'].astype(float) < 0.85) & 
                        (gdf['Peak_34'] < 95)).map({True: 'Y', False: 'N'})
        cols = ['Feeder', 'Peak_34', 'Ready', 'geometry']
    else:
        gdf['Util'] = ((1 - (gdf['HC'].astype(float) / 
                        gdf['Feeder_SN'].astype(float))) * 100).round(1)
        cols = ['Network_ID', 'Util', 'geometry']
    
    gdf = gdf[cols].copy()
    
    # Aggressive simplification (5m tolerance - adjust if needed)
    gdf['geometry'] = gdf['geometry'].simplify(0.00005, preserve_topology=True)
    
    # Save with minimal precision
    gdf.to_file(output_path, driver='GeoJSON', coordinate_precision=4)
    
    # Further compress with gzip
    import gzip
    import shutil
    with open(output_path, 'rb') as f_in:
        with gzip.open(f'{output_path}.gz', 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    
    import os
    original = os.path.getsize(output_path) // 1024
    compressed = os.path.getsize(f'{output_path}.gz') // 1024
    print(f"Original: {original} KB, Gzipped: {compressed} KB ({compressed/original*100:.1f}%)")

optimized_geojson("ri_load_capacity_2025.geojson", "ri_load_screen.json", type='load')
optimized_geojson("ri_hosting_capacity_2025.geojson", "ri_generation_screen.json", type='gen')