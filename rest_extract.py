import requests
import geopandas as gpd
import pandas as pd
import random
import time 

# --- Configuration ---
# The base URL for the MapServer service you provided earlier.
BASE_URL = "https://systemdataportal.nationalgrid.com/arcgis/rest/services/RISDP/RIElecDistFdrsPhases/MapServer"

# List of layers to query:
# 0: Three Phase, OH; 1: Three Phase, UG; 2: Single or Two Phase, OH; 3: Single or Two Phase, UG
LAYER_IDS = [1, 2, 4, 5] # Exclude folders

# Define the output directory and filename
OUTPUT_DIR = r"C:\Users\jack.green.int\incentive_adder_screen\\" 
output_filename = 'ngrid_sample_feeders.geojson'
full_output_path = OUTPUT_DIR + output_filename

# List to hold the GeoDataFrames from each layer
all_feeders = []

print(f"Starting data retrieval from {len(LAYER_IDS)} layers...")

# --- 1. Query and Collect Data from All Layers ---
for layer_id in LAYER_IDS:
    print(f"Querying Layer {layer_id}...")
    
    # Construct the specific query endpoint for the layer
    url = f"{BASE_URL}/{layer_id}/query"
    
    # Common query parameters
    params = {
        'where': '1=1',          # Select all features
        'outFields': '*',        # Return all attributes
        'returnGeometry': 'true',
        'f': 'geojson',          # Request output in GeoJSON format
        'outSR': '4326'          # WGS84 spatial reference
    }
    
    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status() # Raise an exception for bad status codes (4xx or 5xx)
        
        # Read the GeoJSON response into a GeoDataFrame
        feeders_layer = gpd.read_file(response.text)
        
        # Add a column to track the original layer ID (useful for validation)
        feeders_layer['ORIGINAL_LAYER_ID'] = layer_id
        
        all_feeders.append(feeders_layer)
        print(f"  -> Successfully retrieved {len(feeders_layer)} features from Layer {layer_id}.")
        
        time.sleep(1) # Pause to be polite to the server
        
    except requests.exceptions.RequestException as e:
        print(f"  !!! Error querying Layer {layer_id}: {e}")
    except Exception as e:
        print(f"  !!! An unexpected error occurred for Layer {layer_id}: {e}")


# --- 2. Combine All DataFrames ---
if all_feeders:
    # Concatenate all GeoDataFrames into a single one
    # ignore_index=True ensures a clean, sequential index for the combined DataFrame
    feeders_combined = pd.concat(all_feeders, ignore_index=True)
    print(f"\nTotal features combined: {len(feeders_combined)}")
    
    # --- 3. Assign Random Hosting Capacity Values ---
    
    # Generate random integer values between 30 and 100 for each row
    num_features = len(feeders_combined)
    
    # Assign % Generation Hosting Capacity
    feeders_combined['GEN_CAPACITY_PCT'] = [
        random.randint(30, 100) for _ in range(num_features)
    ]
    
    # Assign % Load Hosting Capacity
    feeders_combined['LOAD_CAPACITY_PCT'] = [
        random.randint(30, 100) for _ in range(num_features)
    ]
    
    print("Assigned random hosting capacity values (30-100%) to new fields.")
    
    # --- 4. Simplify Geometry and Export ---
    
    # Simplify geometries to reduce file size and processing load
    # preserve_topology=True helps avoid creating holes or overlaps where none existed
    feeders_combined['geometry'] = feeders_combined.geometry.simplify(0.0001, preserve_topology=True)
    
    # Export the final GeoDataFrame to the specified GeoJSON file path
    feeders_combined.to_file(full_output_path, driver='GeoJSON')
    
    print(f"\nSuccessfully exported data to {full_output_path}")
else:
    print("\nNo features were successfully retrieved. Cannot proceed with merging and exporting.")