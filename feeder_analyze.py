import geopandas as gpd
import pandas as pd
from shapely.geometry import LineString, MultiLineString
from shapely.ops import linemerge
import os

def process_feeder_geometries(gdf, id_col):
    """Groups features by ID, merges line segments, and simplifies geometry."""
    SIMPLIFICATION_TOLERANCE = 0.00001 
    processed = []
    
    for name, group in gdf.groupby(id_col):
        all_geoms = group.geometry.values
        line_geoms_to_merge = []
        
        for geom in all_geoms:
            if geom is None or geom.is_empty:
                continue
            if isinstance(geom, LineString):
                line_geoms_to_merge.append(geom)
            elif isinstance(geom, MultiLineString):
                line_geoms_to_merge.extend(geom.geoms)
        
        if not line_geoms_to_merge:
            continue
            
        merged = linemerge(line_geoms_to_merge)
        if merged is None or merged.is_empty:
            continue
            
        simplified = merged.simplify(SIMPLIFICATION_TOLERANCE, preserve_topology=True) 
        if simplified.is_empty:
            continue

        row_data = group.iloc[0].to_dict()
        row_data['geometry'] = simplified
        processed.append(row_data)

    return gpd.GeoDataFrame(processed, crs=gdf.crs)

def create_load_or_gen_screen(input_path, output_path, data_type):
    """Processes input geospatial data to generate raw data screens without flags."""
    print(f"Reading {input_path}...")
    try:
        gdf = gpd.read_file(input_path)
    except Exception as e:
        print(f"Error reading file {input_path}: {e}")
        return

    if data_type == 'load':
        # Convert 2025 columns to numeric
        gdf['peak_25_mva'] = pd.to_numeric(gdf['FIRST_F2025_Peak_MVA'], errors='coerce')
        gdf['rating_mva'] = pd.to_numeric(gdf['FIRST_Summer_Rating__MVA_'], errors='coerce')
        
        # Calculate loading percentage as a raw numeric metric
        valid_ratings = gdf['rating_mva'].fillna(0) != 0
        gdf['load_pct_25'] = 0.0
        gdf.loc[valid_ratings, 'load_pct_25'] = (
            gdf.loc[valid_ratings, 'peak_25_mva'] / 
            gdf.loc[valid_ratings, 'rating_mva'] * 100
        ).round(1)
        
        cols = ['Feeder', 'peak_25_mva', 'rating_mva', 'load_pct_25', 'geometry']
        id_col = 'Feeder'
        
    elif data_type == 'gen':
        # Extract raw Hosting Capacity in MW
        gdf['hosting_capacity_mw'] = pd.to_numeric(gdf['HC'], errors='coerce').round(2)
        
        cols = ['Network_ID', 'hosting_capacity_mw', 'geometry']
        id_col = 'Network_ID'
    else:
        print(f"Error: Unknown type '{data_type}'.")
        return

    # Filter columns and process geometries
    try:
        gdf = gdf[cols].copy()
    except KeyError as e:
        print(f"Error: Missing required column {e} in input file.")
        return

    print("Merging and simplifying geometries...")
    result_gdf = process_feeder_geometries(gdf, id_col)
    
    # Save output with optimized coordinate precision
    result_gdf.to_file(output_path, driver='GeoJSON', coordinate_precision=4)
    
    if os.path.exists(output_path):
        new_size = os.path.getsize(output_path) / 1024
        print(f"Successfully created {output_path} ({new_size:.1f} KB)")
        print(f"Features: {len(result_gdf)}")
    
    return result_gdf

# --- Execution ---

LOAD_INPUT = "ri_load_capacity_2025.geojson"
GEN_INPUT = "ri_hosting_capacity_2025.geojson"
LOAD_OUTPUT = "ri_load_screen.json"
GEN_OUTPUT = "ri_generation_screen.json"

load_gdf = create_load_or_gen_screen(LOAD_INPUT, LOAD_OUTPUT, data_type='load')
gen_gdf = create_load_or_gen_screen(GEN_INPUT, GEN_OUTPUT, data_type='gen')