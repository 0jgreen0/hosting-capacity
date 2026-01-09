import requests
import geopandas as gpd
import pandas as pd
import os
import json

# --- Configuration ---
LOAD_URL = "https://services.arcgis.com/NTSXKyJwdnK9ffCb/arcgis/rest/services/Distribution_Assets_Overview_and_Load_2025/FeatureServer/0/query"
GEN_URL = "https://services.arcgis.com/NTSXKyJwdnK9ffCb/arcgis/rest/services/RI_Hosting_Capacity_2025/FeatureServer/0/query"

OUTPUT_DIR = r"C:\Users\jack.green.int\incentive_adder_screen"
if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

# Headers to prevent being blocked by the server's security rules
HEADERS = {
    'User-Agent': 'Mozilla/5.0',
    'Content-Type': 'application/x-www-form-urlencoded'
}

def fetch_heavy_data(url, description):
    print(f"\n--- Initializing {description} ---")
    
    # 1. Get Count
    count_resp = requests.get(url, params={'where': '1=1', 'returnCountOnly': 'true', 'f': 'json'}, headers=HEADERS)
    total_records = count_resp.json().get('count', 0)
    print(f"Total records to download: {total_records}")

    all_features = []
    offset = 0
    # Reducing chunk size because feeder line geometry is extremely 'heavy'
    chunk_size = 25 

    while offset < total_records:
        params = {
            'where': '1=1',
            'outFields': '*',
            'resultOffset': offset,
            'resultRecordCount': chunk_size,
            'returnGeometry': 'true',
            'f': 'geojson', # Standard GeoJSON
            'outSR': '4326',
            'quantizationParameters': '' # Disables server-side compression which causes hangs
        }
        
        try:
            # Short timeout, if it fails we will retry with smaller chunks
            response = requests.post(url, data=params, headers=HEADERS, timeout=45)
            response.raise_for_status()
            
            batch_gdf = gpd.read_file(response.text)
            
            if not batch_gdf.empty:
                all_features.append(batch_gdf)
                offset += len(batch_gdf)
                print(f"  -> Progress: {offset} / {total_records} (Chunk successful)")
            else:
                break
                
        except Exception as e:
            print(f"  !!! Chunk failed at offset {offset}. Retrying with smaller batch...")
            chunk_size = max(1, chunk_size // 2) # Cut batch size in half and try again
            if chunk_size < 1:
                break

    if all_features:
        return pd.concat(all_features, ignore_index=True)
    return None

# --- Execution ---

# 1. Get Load Data
load_gdf = fetch_heavy_data(LOAD_URL, "Distribution Load")
if load_gdf is not None:
    # Simplify the resulting geometry locally to make the final file usable
    load_gdf['geometry'] = load_gdf.geometry.simplify(0.0001, preserve_topology=True)
    output_path = os.path.join(OUTPUT_DIR, 'ri_load_capacity_2025.geojson')
    load_gdf.to_file(output_path, driver='GeoJSON')
    print(f"SUCCESS: Saved {len(load_gdf)} feeders to {output_path}")

# 2. Get Hosting Capacity (usually smaller segments, easier to pull)
gen_gdf = fetch_heavy_data(GEN_URL, "Hosting Capacity")
if gen_gdf is not None:
    output_path = os.path.join(OUTPUT_DIR, 'ri_hosting_capacity_2025.geojson')
    gen_gdf.to_file(output_path, driver='GeoJSON')
    print(f"SUCCESS: Saved {len(gen_gdf)} segments to {output_path}")