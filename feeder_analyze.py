import geopandas as gpd
import pandas as pd
import json
from shapely.geometry import shape, mapping

def ultra_slim_geojson(input_path, output_path, type='load'):
    print(f"Reading {input_path}...")
    gdf = gpd.read_file(input_path)

    # 1. Round Coordinates (Massive string reduction)
    def round_coords(geom):
        if geom is None: return None
        g = mapping(geom)
        g['coordinates'] = json.loads(json.dumps(g['coordinates'])) # Float cleaning
        return shape(g)

    # 2. Map-specific Logic
    if type == 'load':
        # Calculations
        gdf['Peak_34'] = (pd.to_numeric(gdf['FIRST_F2034_Peak_MVA'], errors='coerce') / 
                          pd.to_numeric(gdf['FIRST_Summer_Rating__MVA_'], errors='coerce') * 100).round(1)
        gdf['Ready'] = ((pd.to_numeric(gdf['FIRST_F2025_Peak__'], errors='coerce') < 0.85) & 
                        (gdf['Peak_34'] < 95)).map({True: 'Y', False: 'N'})
        cols = ['Feeder', 'Peak_34', 'Ready', 'geometry']
    else:
        gdf['Util'] = ((1 - (pd.to_numeric(gdf['HC'], errors='coerce') / 
                             pd.to_numeric(gdf['Feeder_SN'], errors='coerce'))) * 100).round(1)
        cols = ['Network_ID', 'Util', 'geometry']

    # Keep only essential columns
    gdf = gdf[cols]

    # 3. Geometric Simplification (Tolerance of ~2 meters)
    # This removes 50-80% of the nodes in the LineStrings
    gdf['geometry'] = gdf['geometry'].simplify(0.00002, preserve_topology=True)

    # 4. Save with coordinate precision limiting
    gdf.to_file(output_path, driver='GeoJSON', coordinate_precision=5)
    
    import os
    print(f"Done! {output_path} is now {os.path.getsize(output_path)//1024} KB")

ultra_slim_geojson("ri_load_capacity_2025.geojson", "ri_load_screen.json", type='load')
ultra_slim_geojson("ri_hosting_capacity_2025.geojson", "ri_generation_screen.json", type='gen')