import geopandas as gpd
import pandas as pd
from shapely.geometry import LineString, MultiLineString
from shapely.ops import linemerge
import os

# --- Helper Function to Clean and Merge Geometries (Kept without branch elimination) ---

def process_feeder_geometries(gdf, id_col):
    """Groups features by ID, merges all LineString segments, and simplifies the result."""
    
    SIMPLIFICATION_TOLERANCE = 0.00001 
    processed = []
    
    for name, group in gdf.groupby(id_col):
        all_geoms = group.geometry.values
        
        line_geoms_to_merge = []
        for geom in all_geoms:
            if geom is None or geom.is_empty:
                continue
            elif isinstance(geom, LineString):
                line_geoms_to_merge.append(geom)
            elif isinstance(geom, MultiLineString):
                line_geoms_to_merge.extend(geom.geoms)
        
        if not line_geoms_to_merge:
            continue
            
        combined = linemerge(line_geoms_to_merge)
        geometry_to_simplify = combined 
            
        if geometry_to_simplify is None or geometry_to_simplify.is_empty:
            continue
            
        simplified = geometry_to_simplify.simplify(SIMPLIFICATION_TOLERANCE, preserve_topology=True) 
        
        if simplified.is_empty:
            continue

        row_data = group.iloc[0].to_dict()
        row_data['geometry'] = simplified
        processed.append(row_data)

    return gpd.GeoDataFrame(processed, crs=gdf.crs)

# --- Main Processing Function ---

def create_load_or_gen_screen(input_path, output_path, type):
    """Reads input data, calculates new metrics, and simplifies feeder geometries."""
    
    print(f"Reading {input_path}...")
    try:
        gdf = gpd.read_file(input_path)
    except Exception as e:
        print(f"Error reading file {input_path}: {e}")
        return

    # Calculate fields and filter columns
    if type == 'load':
        # Safely convert required columns to numeric
        gdf['FIRST_F2034_Peak_MVA'] = pd.to_numeric(gdf['FIRST_F2034_Peak_MVA'], errors='coerce')
        gdf['FIRST_Summer_Rating__MVA_'] = pd.to_numeric(gdf['FIRST_Summer_Rating__MVA_'], errors='coerce')
        gdf['FIRST_F2025_Peak_MVA'] = pd.to_numeric(gdf['FIRST_F2025_Peak_MVA'], errors='coerce')
        gdf['FIRST_F2025_Peak__'] = pd.to_numeric(gdf['FIRST_F2025_Peak__'], errors='coerce') 
        
        # Denominator check
        valid_ratings = gdf['FIRST_Summer_Rating__MVA_'].fillna(0) != 0

        # Calculate and RENAME key variables
        gdf['load_peak_34'] = 0.0
        gdf.loc[valid_ratings, 'load_peak_34'] = (gdf.loc[valid_ratings, 'FIRST_F2034_Peak_MVA'] / gdf.loc[valid_ratings, 'FIRST_Summer_Rating__MVA_'] * 100).round(1)

        gdf['load_peak_25'] = 0.0
        gdf.loc[valid_ratings, 'load_peak_25'] = (gdf.loc[valid_ratings, 'FIRST_F2025_Peak_MVA'] / gdf.loc[valid_ratings, 'FIRST_Summer_Rating__MVA_'] * 100).round(1)
        
        # Calculate HP Eligibility Flag (used for TEXT output)
        gdf['HP_Ready'] = ((gdf['FIRST_F2025_Peak__'] < 0.85) & (gdf['load_peak_34'] < 95)).map({True: 'Y', False: 'N'})
        
        cols = ['Feeder', 'load_peak_25', 'load_peak_34', 'HP_Ready', 'geometry']
        id_col = 'Feeder'
        
    elif type == 'gen':
        # Safely convert required columns to numeric
        gdf['HC'] = pd.to_numeric(gdf['HC'], errors='coerce')
        gdf['Feeder_SN'] = pd.to_numeric(gdf['Feeder_SN'], errors='coerce')
        
        valid_sn = gdf['Feeder_SN'].fillna(0) != 0
        
        # Calculate and RENAME key variable
        gdf['gen_util'] = 0.0
        gdf.loc[valid_sn, 'gen_util'] = ((1 - (gdf.loc[valid_sn, 'HC'] / gdf.loc[valid_sn, 'Feeder_SN'])) * 100).round(1)
        
        cols = ['Network_ID', 'gen_util', 'geometry']
        id_col = 'Network_ID'
    else:
        print(f"Error: Unknown type '{type}'.")
        return

    try:
        gdf = gdf[cols].copy()
    except KeyError as e:
        print(f"Error: Missing required column {e} in input file.")
        return

    print("Merging and simplifying geometries...")
    result_gdf = process_feeder_geometries(gdf, id_col)
    
    # Save with minimal precision
    result_gdf.to_file(output_path, driver='GeoJSON', coordinate_precision=4)
    
    # Output file size and feature count comparison
    if os.path.exists(input_path) and os.path.exists(output_path):
        original_size = os.path.getsize(input_path) / 1024
        new_size = os.path.getsize(output_path) / 1024
        
        if original_size > 0:
            print(f"Original: {original_size:.1f} KB → New: {new_size:.1f} KB ({new_size/original_size*100:.1f}%)")
        print(f"Features: {len(gdf)} → {len(result_gdf)}")
    print(f"Successfully created {output_path}")
    return result_gdf


# --- Execution ---

LOAD_INPUT = "ri_load_capacity_2025.geojson"
GEN_INPUT = "ri_hosting_capacity_2025.geojson"
LOAD_OUTPUT = "ri_load_screen.json"
GEN_OUTPUT = "ri_generation_screen.json"

# 1. Create Load and Gen screening files
# We no longer need the STORAGE_OUTPUT file
load_gdf = create_load_or_gen_screen(LOAD_INPUT, LOAD_OUTPUT, type='load')
gen_gdf = create_load_or_gen_screen(GEN_INPUT, GEN_OUTPUT, type='gen')