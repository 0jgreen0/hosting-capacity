import requests
import json

def scrape_rie_system_portal():
    item_id = "b7f446f95c6b4d548d694737c9e66846"
    # Direct access to the Experience Builder underlying JSON data
    data_url = f"https://www.arcgis.com/sharing/rest/content/items/{item_id}/data"
    
    params = {'f': 'json'}
    
    print(f"Connecting to RI Energy Portal (Item ID: {item_id})...")
    
    try:
        response = requests.get(data_url, params=params)
        response.raise_for_status()
        data = response.json()
        
        # Experience Builder config often nests URLs inside 'dataSources'
        sources = data.get('dataSources', {})
        
        print(f"\n{'--- SERVICE NAME ---':<40} | {'--- REST ENDPOINT ---'}")
        
        found = False
        for src_id, config in sources.items():
            # Most relevant info is in 'url' or 'portalUrl'
            service_url = config.get('url')
            label = config.get('label', src_id)
            
            if service_url:
                found = True
                print(f"{label[:38]:<40} | {service_url}")
        
        if not found:
            print("No direct URLs found in dataSources. Checking WebMap references...")
            # If URLs aren't direct, they are inside WebMaps. Let's find those IDs.
            for src_id, config in sources.items():
                if 'itemId' in config:
                    print(f"Found linked WebMap ID: {config['itemId']}")
                    print(f"View Map Layers here: https://www.arcgis.com/sharing/rest/content/items/{config['itemId']}/data?f=json")

    except Exception as e:
        print(f"Error accessing portal: {e}")

if __name__ == "__main__":
    scrape_rie_system_portal()