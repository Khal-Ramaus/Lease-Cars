import pandas as pd
import mysql.connector
import os
from mysql.connector import Error
import json

# Database Config
def load_db_config_from_json(file_path):
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Config file not found: {file_path}")
    with open(file_path, 'r') as f:
        return json.load(f)

CONFIG_FILE = load_db_config_from_json('config.json')

DB_CONFIG = {
    'host': CONFIG_FILE.get('host'),  
    'database': CONFIG_FILE.get('database'), 
    'user': CONFIG_FILE.get('user'),       
    'password': CONFIG_FILE.get('password') 
}

OUTPUT_DIR = 'sql_output'
OUTPUT_FILE = os.path.join(OUTPUT_DIR, 'sql_output.csv')

# .A SQL query that produces a clean, analysis-ready CSV export from your normalized schema

SQL_QUERY = """
SELECT
    dv.leasecarId,
    dv.make,
    dv.model,
    dv.year,
    dv.trimLevel,
    dv.retailPrice,
    dv.fuelType,
    dv.batteryCapacity,
    dv.acceleration,
    dv.topSpeed,
    dv.seats,
    dv.luggageSpace,
    fp.duration AS lease_duration_months,
    fp.mileage AS annual_mileage_km,
    fp.pricePerMonth AS base_price_per_month_eur,
    dc.colorName,
    dc.colorPrice AS color_add_cost_eur
FROM
    dim_vehicles dv 
JOIN
    fact_price fp ON dv.leasecarId = fp.leasecarId
JOIN
    dim_color dc ON dv.leasecarId = dc.leasecarId
ORDER BY
    dv.make, 
    dv.model, 
    fp.duration, 
    fp.mileage;
"""

# Execute query and export to csv
def export_sql_to_csv():
    conn = None
    try:
        # Database Connection
        conn = mysql.connector.connect(**DB_CONFIG)
        if not conn.is_connected():
            print("Failed to connect with database.")
            return

        print("Database Connection Success. Execute Query...")

        # Extract Data with pands
        df = pd.read_sql(SQL_QUERY, conn)

        # Create folder output
        if not os.path.exists(OUTPUT_DIR):
            os.makedirs(OUTPUT_DIR)
            print(f"Folder '{OUTPUT_DIR}' successfully created.")

        # Save Dataframe to CSV
        df.to_csv(OUTPUT_FILE, index=False, encoding='utf-8')

        print(f"\nSUCCESS! {len(df)} rows are export to:")
        print(f"{OUTPUT_FILE}")

    except Error as e:
        print(f"\nError: {e}")
    except Exception as e:
        print(f"\nError Exception: {e}")
    finally:
        if conn and conn.is_connected():
            conn.close()
            print("\nDatabase Connection Closed..")

if __name__ == "__main__":
    export_sql_to_csv()