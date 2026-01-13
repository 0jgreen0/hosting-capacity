import geopandas as gpd
import pandas as pd
from shapely.geometry import LineString, MultiLineString
from shapely.ops import linemerge
import subprocess
import os

def process_feeder_geometries(gdf, id_col, tolerance=0.00005):
    """Groups features by ID, merges line segments, and simplifies geometry."""
    SIMPLIFICATION_TOLERANCE = tolerance 
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
        # Calculate derived values
        peak_25 = pd.to_numeric(gdf['FIRST_F2025_Peak_MVA'], errors='coerce')
        rating = pd.to_numeric(gdf['FIRST_Summer_Rating__MVA_'], errors='coerce')
        
        valid_ratings = rating.fillna(0) != 0
        gdf['load_pct_25'] = 0.0
        gdf.loc[valid_ratings, 'load_pct_25'] = (
            peak_25.loc[valid_ratings] / 
            rating.loc[valid_ratings] * 100
        ).round(1)
        
        gdf['constrained'] = (gdf['load_pct_25'] > 90).astype(int)
        
        # Removed 'peak_25_mva' and 'rating_mva' from the final export columns
        cols = ['Feeder', 'load_pct_25', 'constrained', 'geometry']
        id_col = 'Feeder'
        
    elif data_type == 'gen':
        gdf['hosting_capacity_mw'] = pd.to_numeric(gdf['HC'], errors='coerce').round(2)
        gdf['constrained'] = (gdf['hosting_capacity_mw'] < 0.5).astype(int)
        
        cols = ['Section_ID', 'Network_ID', 'hosting_capacity_mw', 'constrained', 'geometry']
        id_col = 'Section_ID'
    else:
        print(f"Error: Unknown type '{data_type}'.")
        return None

    try:
        gdf = gdf[cols].copy()
    except KeyError as e:
        print(f"Error: Missing required column {e} in input file.")
        return None

    print("Merging and simplifying geometries...")
    tol = 0.0001 if data_type == 'load' else 0.00005
    result_gdf = process_feeder_geometries(gdf, id_col, tolerance=tol)
    
    if result_gdf.crs != "EPSG:4326":
        result_gdf = result_gdf.to_crs("EPSG:4326")

    # Rounding function to reduce text volume in GeoJSON
    def round_coords(geom, precision=6):
        if geom.is_empty: return geom
        return type(geom)([[round(x, precision) for x in pt] for pt in geom.coords])

    # Apply coordinate rounding
    result_gdf.geometry = result_gdf.geometry.map(
        lambda g: round_coords(g, 6) if isinstance(g, LineString) else g
    )

    print(f"Saving optimized GeoJSON to {output_path}...")
    result_gdf.to_file(output_path, driver='GeoJSON')
    
    if os.path.exists(output_path):
        size_mb = os.path.getsize(output_path) / (1024 * 1024)
        print(f"Created {output_path} ({size_mb:.2f} MB, {len(result_gdf)} features)")
    
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
        '--no-feature-limit',           # Prevents dropping features in dense areas
        '--no-tile-size-limit',          # Prevents dropping features to stay under 500kb
        '--no-line-simplification',     # Stops lines from "dashing" or fragmenting
        '--coalesce-smallest-as-needed', # Merges small features to reduce tile size
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
        print("✗ Tippecanoe not found.")
        return False

# --- Execution ---

LOAD_INPUT = "ri_load_capacity_2025.geojson"
GEN_INPUT = "ri_hosting_capacity_2025.geojson"

LOAD_GEOJSON = "ri_load_screen.geojson"
GEN_GEOJSON = "ri_generation_screen.geojson"

LOAD_PMTILES = "ri_load_screen.pmtiles"
GEN_PMTILES = "ri_generation_screen.pmtiles"

print("=" * 60)
print("STEP 1: Processing Load Data")
print("=" * 60)
load_gdf = create_load_or_gen_screen(LOAD_INPUT, LOAD_GEOJSON, data_type='load')

print("\n" + "=" * 60)
print("STEP 2: Processing Generation Data")
print("=" * 60)
gen_gdf = create_load_or_gen_screen(GEN_INPUT, GEN_GEOJSON, data_type='gen')

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