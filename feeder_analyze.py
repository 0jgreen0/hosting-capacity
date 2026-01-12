import geopandas as gpd
import pandas as pd
from shapely.geometry import LineString, MultiLineString
from shapely.ops import linemerge
import os

def remove_branches_and_simplify(input_path, output_path, type='load'):
    """
    Reads a GeoJSON network file, calculates attributes, groups features by ID,
    merges connected LineString segments while removing branches (keeping the 
    longest trunk), simplifies the geometry, and saves the result.
    
    Handles input geometries that are LineString or MultiLineString.
    """
    print(f"Reading {input_path}...")
    try:
        gdf = gpd.read_file(input_path)
    except Exception as e:
        print(f"Error reading file {input_path}: {e}")
        return

    # Calculate fields and filter columns
    if type == 'load':
        # Ensure columns exist and handle potential NaN/non-numeric values before astype(float)
        # Using .loc and pd.to_numeric for safer conversion
        gdf['FIRST_F2034_Peak_MVA'] = pd.to_numeric(gdf['FIRST_F2034_Peak_MVA'], errors='coerce')
        gdf['FIRST_Summer_Rating__MVA_'] = pd.to_numeric(gdf['FIRST_Summer_Rating__MVA_'], errors='coerce')
        gdf['FIRST_F2025_Peak__'] = pd.to_numeric(gdf['FIRST_F2025_Peak__'], errors='coerce')

        # Calculate Peak_34, handling division by zero/NaN rating
        valid_ratings = gdf['FIRST_Summer_Rating__MVA_'].fillna(0) != 0
        gdf['Peak_34'] = 0.0 # Default value
        gdf.loc[valid_ratings, 'Peak_34'] = (
            gdf.loc[valid_ratings, 'FIRST_F2034_Peak_MVA'] / 
            gdf.loc[valid_ratings, 'FIRST_Summer_Rating__MVA_'] * 100
        ).round(1)

        # Calculate Ready
        gdf['Ready'] = ((gdf['FIRST_F2025_Peak__'] < 0.85) & 
                        (gdf['Peak_34'] < 95)).map({True: 'Y', False: 'N'})
        
        cols = ['Feeder', 'Peak_34', 'Ready', 'geometry']
        id_col = 'Feeder'
    else: # type == 'gen'
        gdf['HC'] = pd.to_numeric(gdf['HC'], errors='coerce')
        gdf['Feeder_SN'] = pd.to_numeric(gdf['Feeder_SN'], errors='coerce')

        # Calculate Util, handling division by zero/NaN rating
        valid_sn = gdf['Feeder_SN'].fillna(0) != 0
        gdf['Util'] = 0.0 # Default value
        gdf.loc[valid_sn, 'Util'] = (
            (1 - (gdf.loc[valid_sn, 'HC'] / 
                  gdf.loc[valid_sn, 'Feeder_SN'])) * 100
        ).round(1)
        
        cols = ['Network_ID', 'Util', 'geometry']
        id_col = 'Network_ID'

    # Select required columns and create a copy
    # Note: Use a try-except/check to ensure all required columns exist in the input file
    try:
        gdf = gdf[cols].copy()
    except KeyError as e:
        print(f"Error: Missing required column {e} in input file.")
        return

    # Group by feeder/network and merge geometries
    print("Merging and simplifying geometries...")
    
    processed = []
    
    # Iterate through groups
    for name, group in gdf.groupby(id_col):
        all_geoms = group.geometry.values
        
        # **Crucial Step: Flatten MultiLineStrings and filter to only individual LineString objects**
        line_geoms_to_merge = []
        for geom in all_geoms:
            if geom is None or geom.is_empty:
                continue
            elif isinstance(geom, LineString):
                line_geoms_to_merge.append(geom)
            elif isinstance(geom, MultiLineString):
                # Extract individual LineString components from the MultiLineString
                line_geoms_to_merge.extend(geom.geoms)
            # Ignore other geometry types (Point, Polygon)
        
        # Check if there are any valid LineStrings left to merge
        if not line_geoms_to_merge:
            # print(f"Warning: No valid LineString geometries found for {id_col}={name}. Skipping.")
            continue
            
        # Merge all individual LineStrings for this feeder/network
        # This resolves the original NotImplementedError
        combined = linemerge(line_geoms_to_merge)
        
        # Keep only main trunk (longest segment)
        if isinstance(combined, MultiLineString):
            # If linemerge returns multiple segments, keep the longest one (main trunk)
            main_trunk = max(combined.geoms, key=lambda x: x.length)
        elif isinstance(combined, LineString):
            main_trunk = combined
        else:
            # Handle cases where linemerge returns other types (e.g., Point if input was just points)
            # print(f"Warning: Merged geometry is not LineString/MultiLineString for {id_col}={name}. Skipping.")
            continue
            
        # Simplify the main trunk
        simplified = main_trunk.simplify(0.0001, preserve_topology=True)
        
        if simplified.is_empty:
            # print(f"Warning: Simplified geometry is empty for {id_col}={name}. Skipping.")
            continue

        # Take first row's properties and assign the new simplified geometry
        row_data = group.iloc[0].to_dict()
        row_data['geometry'] = simplified
        processed.append(row_data)

    result_gdf = gpd.GeoDataFrame(processed, crs=gdf.crs)
    
    # Save with minimal precision
    result_gdf.to_file(output_path, driver='GeoJSON', coordinate_precision=4)
    
    # Output file size and feature count comparison
    if os.path.exists(input_path) and os.path.exists(output_path):
        original_size = os.path.getsize(input_path) / 1024
        new_size = os.path.getsize(output_path) / 1024
        
        if original_size > 0:
            print(f"Original: {original_size:.1f} KB → New: {new_size:.1f} KB ({new_size/original_size*100:.1f}%)")
        else:
            print(f"Original file size is 0 KB.")

    print(f"Features: {len(gdf)} → {len(result_gdf)}")
    print(f"Successfully created {output_path}")

# --- Example Usage (Using your original lines) ---
print("--- Processing ri_load_capacity_2025.geojson ---")
remove_branches_and_simplify("ri_load_capacity_2025.geojson", "ri_load_screen.json", type='load')

print("\n--- Processing ri_hosting_capacity_2025.geojson ---")
remove_branches_and_simplify("ri_hosting_capacity_2025.geojson", "ri_generation_screen.json", type='gen')