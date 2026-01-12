import geopandas as gpd
import pandas as pd
from shapely.geometry import LineString, MultiLineString
from shapely.ops import linemerge
import subprocess
import json
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
    """Processes input geospatial data to generate GeoJSON for tiling."""
    print(f"Reading {input_path}...")
    try:
        gdf = gpd.read_file(input_path)
    except Exception as e:
        print(f"Error reading file {input_path}: {e}")
        return None

    if data_type == 'load':
        gdf['peak_25_mva'] = pd.to_numeric(gdf['FIRST_F2025_Peak_MVA'], errors='coerce')
        gdf['rating_mva'] = pd.to_numeric(gdf['FIRST_Summer_Rating__MVA_'], errors='coerce')
        
        valid_ratings = gdf['rating_mva'].fillna(0) != 0
        gdf['load_pct_25'] = 0.0
        gdf.loc[valid_ratings, 'load_pct_25'] = (
            gdf.loc[valid_ratings, 'peak_25_mva'] / 
            gdf.loc[valid_ratings, 'rating_mva'] * 100
        ).round(1)
        
        # Add constraint flag for styling
        gdf['constrained'] = (gdf['load_pct_25'] > 90).astype(int)
        
        cols = ['Feeder', 'peak_25_mva', 'rating_mva', 'load_pct_25', 'constrained', 'geometry']
        id_col = 'Feeder'
        
    elif data_type == 'gen':
        gdf['hosting_capacity_mw'] = pd.to_numeric(gdf['HC'], errors='coerce').round(2)
        gdf['constrained'] = (gdf['hosting_capacity_mw'] < 0.5).astype(int)
        
        cols = ['Network_ID', 'hosting_capacity_mw', 'constrained', 'geometry']
        id_col = 'Network_ID'
    else:
        print(f"Error: Unknown type '{data_type}'.")
        return None

    try:
        gdf = gdf[cols].copy()
    except KeyError as e:
        print(f"Error: Missing required column {e} in input file.")
        return None

    print("Merging and simplifying geometries...")
    result_gdf = process_feeder_geometries(gdf, id_col)
    
    # Ensure WGS84 for web mapping
    if result_gdf.crs != "EPSG:4326":
        result_gdf = result_gdf.to_crs("EPSG:4326")
    
    # Save as GeoJSON (input for tippecanoe)
    result_gdf.to_file(output_path, driver='GeoJSON')
    
    if os.path.exists(output_path):
        size_kb = os.path.getsize(output_path) / 1024
        print(f"Created {output_path} ({size_kb:.1f} KB, {len(result_gdf)} features)")
    
    return result_gdf

def create_pmtiles(geojson_path, pmtiles_path, layer_name, min_zoom=7, max_zoom=14):
    """Convert GeoJSON to PMTiles using tippecanoe."""
    print(f"\nGenerating PMTiles from {geojson_path}...")
    
    cmd = [
        'tippecanoe',
        '-o', pmtiles_path,
        '-l', layer_name,
        '-Z', str(min_zoom),
        '-z', str(max_zoom),
        '--drop-densest-as-needed',
        '--extend-zooms-if-still-dropping',
        '--force',
        geojson_path
    ]
    
    try:
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        if os.path.exists(pmtiles_path):
            size_kb = os.path.getsize(pmtiles_path) / 1024
            print(f"✓ Created {pmtiles_path} ({size_kb:.1f} KB)")
        return True
    except subprocess.CalledProcessError as e:
        print(f"✗ Tippecanoe error: {e.stderr}")
        return False
    except FileNotFoundError:
        print("✗ Tippecanoe not found. Install with: brew install tippecanoe (macOS) or see https://github.com/felt/tippecanoe")
        return False

# --- Execution ---

LOAD_INPUT = "ri_load_capacity_2025.geojson"
GEN_INPUT = "ri_hosting_capacity_2025.geojson"

# Intermediate GeoJSON files
LOAD_GEOJSON = "ri_load_screen.geojson"
GEN_GEOJSON = "ri_generation_screen.geojson"

# Final PMTiles outputs
LOAD_PMTILES = "ri_load_screen.pmtiles"
GEN_PMTILES = "ri_generation_screen.pmtiles"

# Process data
print("=" * 60)
print("STEP 1: Processing Load Data")
print("=" * 60)
load_gdf = create_load_or_gen_screen(LOAD_INPUT, LOAD_GEOJSON, data_type='load')

print("\n" + "=" * 60)
print("STEP 2: Processing Generation Data")
print("=" * 60)
gen_gdf = create_load_or_gen_screen(GEN_INPUT, GEN_GEOJSON, data_type='gen')

# Create PMTiles
if load_gdf is not None:
    print("\n" + "=" * 60)
    print("STEP 3: Creating Load PMTiles")
    print("=" * 60)
    create_pmtiles(LOAD_GEOJSON, LOAD_PMTILES, 'load_capacity')

if gen_gdf is not None:
    print("\n" + "=" * 60)
    print("STEP 4: Creating Generation PMTiles")
    print("=" * 60)
    create_pmtiles(GEN_GEOJSON, GEN_PMTILES, 'hosting_capacity')

print("\n" + "=" * 60)
print("COMPLETE")
print("=" * 60)
print("\nUpload these files to your hosting:")
print(f"  - {LOAD_PMTILES}")
print(f"  - {GEN_PMTILES}")