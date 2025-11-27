import requests
import json
import pandas as pd
import warnings
import os

warnings.filterwarnings("ignore")

# Extract leasecarId from privatelease API
url = "https://api.anwb.nl/v2/privatelease"

headers = {
    "accept": "*/*",
    "content-type": "application/x-www-form-urlencoded;charset=UTF-8",
    "origin": "https://www.anwb.nl",
    "referer": "https://www.anwb.nl/",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36",
    "x-anwb-client-id": "86AhuFzAtaqLVRGLpfk7SGboVcLzCsnQ",
}

data = {
    "q": "*:*",
    "sort": "exists(offerText) desc, price asc",
    "start": "0",
    "rows": "1000", 
    "fq": "{!tag=beforeCollapseTag}{!collapse field=productGroup sort='price asc, duration desc'}",
    "json": json.dumps({ 
        "facet": {
            "manufacturer": { "type": "terms", "field": "manufacturer", "limit": 100, "domain": {"excludeTags": "manufacturer"}, "sort": { "index": "asc" }, "mincount": 0 },
            "fuelType": { "type": "terms", "field": "fuelType", "domain": {"excludeTags": "fuelType"}, "sort": { "index": "asc" }, "mincount": 0 },
        } 
    }),
    "facet": "true",
    "facet.pivot": "{!ex=beforeCollapseTag key=uniqueCarsPerProductGroup}productGroup,leasecarId"
}

leaseid_resp = requests.post(url, headers=headers, data=data)
data_json = leaseid_resp.json()
leasecarIds = [item["leasecarId"] for item in data_json.get("items", [])]

print(f"API leasecar status: {leaseid_resp.status_code}")
print(f"Found {len(leasecarIds)} Lease Car Id: {leasecarIds}")

# Initialize Lists 
all_vehicle_details = []
all_lease_prices = []
all_colors = []

# Itterate for the second API which is the lease car detail

headers_detail = {
    "accept": "application/json",
    "apikey": "8pV4D410gARy3ZlXaH2jeFdtE56LX58x", 
    "origin": "https://www.anwb.nl",
    "referer": "https://www.anwb.nl/",
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36",
}

# Set Random delays time to make it behave like human 
import time
import random

min_delay = 4.5  # Minimum Sec
max_delay = 15.0  # Maximum Sec

for leasecarId in leasecarIds:
    random_delay = random.uniform(min_delay, max_delay)
    print(f"----- Waiting for {random_delay:.2f} seconds ------")
    time.sleep(random_delay)
    print(f"-> Extract detail of leasecarId: {leasecarId}")

    url_detail = f"https://api.anwb.nl/privatelease/v1/leaseplan/leasecars/{leasecarId}"
    detail_resp = requests.get(url_detail, headers=headers_detail)
    
    if detail_resp.status_code != 200:
        print(f"Failed to extract for ID {leasecarId}, Status: {detail_resp.status_code}")
        continue
    
    try:
        data = detail_resp.json()
    except json.JSONDecodeError:
        print(f"Failed to parse JSON for ID: {leasecarId}.")
        continue

    vehicle_data = data.get('vehicleData', {})
    lease_data = data.get('privateLeaseData', {})
    

    base_keys = {
        "id": data.get('id'),
        "make": vehicle_data.get('make'),
        "model": vehicle_data.get('model'),
    }

    # Combine data of specs
    detail_row = {
        "id": data.get('id'),
        "make": vehicle_data.get('make'),
        "model": vehicle_data.get('model'),
        "year": vehicle_data.get('year'),
        "type": vehicle_data.get('type'),
        "trimLevel": vehicle_data.get('trimLevel'),
        "retailPrice": float(vehicle_data.get('retailPrice', 0.0)),
        "fuelType": vehicle_data.get('fuelType'),
        "batteryCapacity": float(vehicle_data.get('batteryCapacity', 0.0)),
        "range": float(vehicle_data.get('range', 0.0)),
        "enginePowerHP": int(vehicle_data.get('enginePowerHP', 0)),
        "maxTorque": int(vehicle_data.get('maxTorque', 0)),
        "acceleration": float(vehicle_data.get('acceleration', 0.0)),
        "topSpeed": int(vehicle_data.get('topSpeed', 0)),
        "length": int(vehicle_data.get('length', 0)),
        "height": int(vehicle_data.get('height', 0)),
        "weight": int(vehicle_data.get('weight', 0)),
        "seats": int(vehicle_data.get('seats', 0)),
        "luggageSpace": int(vehicle_data.get('luggageSpace', 0)),
        
        "standardFeatures_list": "|".join([
             f"{item.get('group')}: {item.get('name')}" 
             for item in vehicle_data.get('standardEquipment', [])
        ])
    }
    all_vehicle_details.append(detail_row)

    # Get Lease Price
    for point in lease_data.get('pricePoints', []):
        lease_row = {
            "leasecarId": base_keys['id'],
            "make": base_keys['make'],
            "model": base_keys['model'],
            "duration": point.get('duration'), # Durasi (bulan)
            "mileage": point.get('mileage'), # Jarak Tempuh (km)
            "pricePerMonth": point.get('pricePerMonth') # Harga per bulan
        }
        all_lease_prices.append(lease_row)

    # Get Color Data
    for color in lease_data.get('colors', []):
        color_row = {
            "leasecarId": base_keys['id'],
            "make": base_keys['make'],
            "model": base_keys['model'],
            "colorName": color.get('name'),
            "colorCode": color.get('orderCode'),
            "colorPrice": color.get('price'),
            "primaryRgbCode": color.get('primaryRgbCode'),
        }
        all_colors.append(color_row)



# Convert to Dataframe

OUTPUT_DIR = 'scraped_data'

# Generate new folder if it doesnt exist
if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)
    print(f"📁 Folder '{OUTPUT_DIR}' is created.")


# DataFrame 1: Specification
df_details = pd.DataFrame(all_vehicle_details)
# Save to 'scraped_data/data_specs.csv'
df_details.to_csv(os.path.join(OUTPUT_DIR, 'data_specs.csv'), index=False)

# DataFrame 2: Lease Price
df_prices = pd.DataFrame(all_lease_prices)
df_prices.to_csv(os.path.join(OUTPUT_DIR, 'data_price.csv'), index=False)

# DataFrame 3: Color options
df_colors = pd.DataFrame(all_colors)
df_colors.to_csv(os.path.join(OUTPUT_DIR, 'data_color.csv'), index=False)



print("\n" + "="*70)
print("Dataframes saved to CSV files (3 CSVs created)")
print(f"All files is stored to folder {OUTPUT_DIR}.")

# Data Example
try:
    print("\ndata_specs.csv Columns:")
    print(df_details.head().to_markdown(index=False))
except ImportError:
    print("\nINFO: No module 'tabulate'. Please Install (`pip install tabulate`) to see preview tabel.")