import geopandas as gpd
from shapely.geometry import Point
import pandas as pd
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter

# --- Configuration ---
FEEDER_FILE = 'ngrid_sample_feeders.geojson'
INCLUSION_FILE = 'inclusion_map/inclusion_map.geojson' # Your file is now an inclusion boundary
OUTPUT_FILE = 'address_eligibility_report.csv'
PROJECTED_CRS = 'EPSG:32619' 
CAPACITY_THRESHOLD_PCT = 90 # <<< NEW: EASY TO CHANGE THRESHOLD
ADDRESSES_TO_TEST = [
    "150 Washington St, Providence, RI",
    "98 Pascoag Main St, Pascoag, RI", # Should not pass (outside area)
    "12 Ocean Ave, New Shoreham, RI" # Should not pass (outside area)
]

# --- 1. Load and Prepare Data ---
print("Loading spatial data...")
try:
    feeders = gpd.read_file(FEEDER_FILE)
    if 'GEN_CAPACITY_PCT' not in feeders.columns or 'LOAD_CAPACITY_PCT' not in feeders.columns:
        raise ValueError(f"Missing capacity columns in {FEEDER_FILE}")
        
    # **INCLUSION LOGIC MODIFICATION**
    inclusion_zones = gpd.read_file(INCLUSION_FILE).to_crs('EPSG:4326')
    # Dissolve all inclusion features into a single geometry for efficient checking
    inclusion_boundary = inclusion_zones.dissolve().geometry.iloc[0]
    
    print(f"Loaded {len(feeders)} feeders. Created single inclusion boundary.")
except FileNotFoundError as e:
    print(f"FATAL ERROR: Could not find required file: {e}")
    exit()
except ValueError as e:
    print(f"FATAL ERROR: Data validation failed: {e}")
    exit()


# Pre-project feeders to the local CRS for distance calculation
feeders_proj = feeders.to_crs(PROJECTED_CRS) 

# --- 2. Geocoding Setup (Used only for testing addresses) ---
geolocator = Nominatim(user_agent="incentive_adder_screen_tool")
geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1.5)


def check_eligibility(lat, lon):
    """
    Checks eligibility for a single coordinate point based on INCLUSION zone,
    nearest feeder capacity, and specific incentive screening logic.
    """
    point = Point(lon, lat)
    
    # --- Check 1: Inclusion Zone (REVERSED LOGIC) ---
    # If the point is NOT contained by the single inclusion boundary, it is ineligible.
    if not inclusion_boundary.contains(point):
        return {
            'overall_eligible': 'no',
            'reason': 'Address is OUTSIDE the service/inclusion area.',
            'distance_feet': 'N/A',
            'GEN_CAPACITY_PCT': 'N/A',
            'LOAD_CAPACITY_PCT': 'N/A',
            'battery_incentive_eligible': 'no',
            'heat_pump_incentive_eligible': 'no'
        }

    # --- Check 2: Nearest Feeder Analysis ---
    point_gdf = gpd.GeoDataFrame([{'geometry': point}], crs='EPSG:4326')
    point_proj = point_gdf.to_crs(PROJECTED_CRS)
    
    # Calculate distances
    distances = feeders_proj.distance(point_proj.geometry.iloc[0])
    nearest_idx = distances.idxmin()
    nearest_feeder = feeders.iloc[nearest_idx]
    
    distance_meters = distances.min()
    distance_feet = distance_meters * 3.28084
    
    gen_cap = nearest_feeder['GEN_CAPACITY_PCT']
    load_cap = nearest_feeder['LOAD_CAPACITY_PCT']
    
    # --- Check 3: Incentive-Specific Screening Logic ---
    
    # **Battery Incentive Logic:** Eligible if BOTH Gen Capacity and Load Capacity are < THRESHOLD
    battery_eligible = (gen_cap < CAPACITY_THRESHOLD_PCT) and (load_cap < CAPACITY_THRESHOLD_PCT)

    # **Heat Pump Incentive Logic:** Eligible if ONLY Load Capacity is < THRESHOLD
    heat_pump_eligible = (load_cap < CAPACITY_THRESHOLD_PCT)

    # --- Check 4: Final Results Compilation ---
    return {
        'overall_eligible': 'yes', # Within service area
        'reason': 'Within service area.',
        'feeder_id': nearest_feeder.get('FEEDER_ID', 'N/A'),
        'distance_feet': round(distance_feet, 1),
        'GEN_CAPACITY_PCT': gen_cap,
        'LOAD_CAPACITY_PCT': load_cap,
        'battery_incentive_eligible': 'yes' if battery_eligible else 'no',
        'heat_pump_incentive_eligible': 'yes' if heat_pump_eligible else 'no'
    }


def process_addresses(addresses):
    """
    Geocodes a list of addresses and performs the eligibility check on each.
    """
    results = []
    print(f"\n--- 3. Geocoding and Processing Addresses (Threshold: < {CAPACITY_THRESHOLD_PCT}%) ---")
    for address in addresses:
        print(f"Processing: {address}...")
        
        # Geocode the address
        location = geocode(address)
        
        if location:
            lat, lon = location.latitude, location.longitude
            eligibility_result = check_eligibility(lat, lon)
            
            # Combine address and results
            result = {
                'address': address,
                'latitude': lat,
                'longitude': lon,
                **eligibility_result # Unpack the result dictionary
            }
            results.append(result)
            print(f"  -> HP Eligible: {eligibility_result['heat_pump_incentive_eligible']} | Battery Eligible: {eligibility_result['battery_incentive_eligible']} | Gen Cap: {eligibility_result['GEN_CAPACITY_PCT']}% | Load Cap: {eligibility_result['LOAD_CAPACITY_PCT']}%")
        else:
            print(f"  !!! Geocoding failed for: {address}")
            results.append({
                'address': address,
                'latitude': 'N/A',
                'longitude': 'N/A',
                'overall_eligible': 'error',
                'reason': 'Geocoding failed',
                'distance_feet': 'N/A',
                'GEN_CAPACITY_PCT': 'N/A',
                'LOAD_CAPACITY_PCT': 'N/A',
                'battery_incentive_eligible': 'error',
                'heat_pump_incentive_eligible': 'error'
            })
            
    return pd.DataFrame(results)

# --- 4. Execution ---
final_report_df = process_addresses(ADDRESSES_TO_TEST)

# Save the final report
final_report_df.to_csv(OUTPUT_FILE, index=False)
print(f"\n--- 4. Final Report ---")
print(f"Report saved to {OUTPUT_FILE}")
print(final_report_df[['address', 'overall_eligible', 'battery_incentive_eligible', 'heat_pump_incentive_eligible', 'reason', 'distance_feet', 'GEN_CAPACITY_PCT', 'LOAD_CAPACITY_PCT']])