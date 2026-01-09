import requests
import geopandas as gpd
import pandas as pd
import random
import time 

# --- Configuration ---
BASE_URL = "https://systemdataportal.nationalgrid.com/arcgis/rest/services/RISDP/RIElecDistFdrsPhases/MapServer"
LAYER_IDS = [1, 2, 4, 5] 

OUTPUT_DIR = r"C:\Users\jack.green.int\incentive_adder_screen\\" 
output_filename = 'ngrid_sample_feeders.geojson'
full_output_path = OUTPUT_DIR + output_filename

all_feeders = []

print(f"Starting data retrieval from {len(LAYER_IDS)} layers...")

# --- 1. Query and Collect Data ---
for layer_id in LAYER_IDS:
    print(f"Querying Layer {layer_id}...")
    url = f"{BASE_URL}/{layer_id}/query"
    params = {
        'where': '1=1',
        'outFields': '*',
        'returnGeometry': 'true',
        'f': 'geojson',
        'outSR': '4326'
    }
    
    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        
        feeders_layer = gpd.read_file(response.text)
        feeders_layer['ORIGINAL_LAYER_ID'] = layer_id
        all_feeders.append(feeders_layer)
        print(f"  -> Retrieved {len(feeders_layer)} features.")
        time.sleep(1) 
        
    except Exception as e:
        print(f"  !!! Error querying Layer {layer_id}: {e}")

# --- 2. Combine and Assign Capacity ---
if all_feeders:
    feeders_combined = pd.concat(all_feeders, ignore_index=True)
    
    # Using OBJECTID as the primary spatial key
    ID_COL = 'OBJECTID' 
    
    # Identify the logical Feeder Name column for consistent capacity
    # This ensures that all segments of the same feeder get the same random value
    name_cols = ['Feeder_ID', 'FEEDERID', 'FeederNum', 'FEEDER_ID']
    NAME_COL = next((c for c in name_cols if c in feeders_combined.columns), ID_COL)
    
    print(f"Assigning consistent capacity based on: {NAME_COL}")

    # Create consistent capacity lookup
    unique_names = feeders_combined[NAME_COL].unique()
    capacity_lookup = pd.DataFrame({
        NAME_COL: unique_names,
        'GEN_CAPACITY_PCT': [random.randint(30, 100) for _ in range(len(unique_names))],
        'LOAD_CAPACITY_PCT': [random.randint(30, 100) for _ in range(len(unique_names))]
    })
    
    # Merge capacity values back to segments
    feeders_combined = feeders_combined.merge(capacity_lookup, on=NAME_COL, how='left')

    # --- 3. Dissolve Segments (The Fix for Choppiness) ---
    print(f"Dissolving segments to preserve layers: {LAYER_IDS}")
    
    # We dissolve by the NAME_COL and LAYER_ID to merge segments into single paths
    feeders_dissolved = feeders_combined.dissolve(
        by=[NAME_COL, 'ORIGINAL_LAYER_ID'], 
        aggfunc='first'
    ).reset_index()

    # --- 4. Export ---
    feeders_dissolved['geometry'] = feeders_dissolved.geometry.simplify(0.0001, preserve_topology=True)
    feeders_dissolved.to_file(full_output_path, driver='GeoJSON')
    
    print(f"\nFinal count: {len(feeders_dissolved)} unified feeder-layer objects.")
    print(f"Successfully exported to {full_output_path}")
else:
    print("\nNo data retrieved.")