import pandas as pd
import mysql.connector
from mysql.connector import Error
import os
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

CSV_FILES = {
    'dim_vehicles': 'scraped_data/data_specs.csv',
    'fact_price': 'scraped_data/data_price.csv',
    'dim_color': 'scraped_data/data_color.csv',
}

# Database Connection

def connect_to_db():
    """Connection database MySQL."""
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        if conn.is_connected():
            print("Database Connection success!")
            return conn
    except Error as e:
        print(f"Error to connect to MySQL: {e}")
        return None

def get_existing_leasecar_ids(conn):
    """Extract leasecarId from dim_vehicles."""
    print("🔎 Extract leasecarId from dim_vehicles...")
    cursor = conn.cursor()
    try:
        # Use query select
        cursor.execute("SELECT leasecarId FROM dim_vehicles")
        # Convert to set
        valid_ids = {row[0] for row in cursor.fetchall()}
        print(f"Found {len(valid_ids)} Valid Lease car ID.")
        return valid_ids
    except Error as e:
        print(f"Failed to get ID from dim_vehicles: {e}")
        return set()
    finally:
        cursor.close()

# Insert Data to Database

def insert_data(conn, table_name, csv_file, chunk_size=1000, valid_ids=None):
    """Read CSV and Insert to tabel MySQL."""
    
    rename_map = {}
    
    if table_name == 'dim_vehicles':
        columns = [
            'leasecarId', 'make', 'model', 'year', 'type', 'trimLevel', 'retailPrice',
            'fuelType', 'batteryCapacity', '`range`', 'enginePowerHP', 'maxTorque', 
            'acceleration', 'topSpeed', 'length', 'height', 'weight', 'seats', 
            'luggageSpace', 'standardFeatures_list'
        ]
        # Mapping header CSV 'id' to MYSQL column 'leasecarId'
        rename_map = {'id': 'leasecarId', 'range': '`range`'}
    
    elif table_name == 'fact_price':
        columns = ['leasecarId', 'duration', 'mileage', 'pricePerMonth']
        cols_to_drop = ['make', 'model']
        
    elif table_name == 'dim_color':
        columns = ['leasecarId', 'colorName', 'colorCode', 'colorPrice', 'primaryRgbCode']
        cols_to_drop = ['make', 'model']
        
    else:
        print(f"Tabel {table_name} is Not Found.")
        return

    # Generate string template for INSERT SQL
    cols_str = ', '.join(columns)
    placeholders = ', '.join(['%s'] * len(columns))
    insert_query = f"INSERT INTO {table_name} ({cols_str}) VALUES ({placeholders})"
    
    cursor = conn.cursor()
    total_rows = 0
    rows_filtered = 0
    
    try:
        print(f"\n--- Load {csv_file} to {table_name}...")
        
        # Read CSV, Ignore unnamed column
        for chunk in pd.read_csv(csv_file, chunksize=chunk_size, usecols=lambda x: x not in ['Unnamed: 0']):
            
            # 1. Replace column name in CSV (id -> leasecarId, range -> `range`)
            if rename_map:
                chunk.rename(columns=rename_map, inplace=True)
            
            # Only store valid leasecarId (in dim_vehicles) from price
            if table_name == 'fact_price' and valid_ids is not None:
                initial_count = len(chunk)
                chunk = chunk[chunk['leasecarId'].isin(valid_ids)]
                rows_filtered += initial_count - len(chunk)
            
            # 2. Cleaning DataFrame (remove column 'make' and 'model')
            cols_to_drop_in_chunk = [col for col in ['make', 'model'] if col in chunk.columns and col not in columns]
            chunk = chunk.drop(columns=cols_to_drop_in_chunk, errors='ignore')

            # 3. Replace NaN with None
            chunk = chunk.where(pd.notna(chunk), None)
            
            # 4. Sorting
            data_to_insert = [tuple(row) for row in chunk[columns].values]
            
            # 5. Exectute Insert Query
            cursor.executemany(insert_query, data_to_insert)
            conn.commit()
            total_rows += len(data_to_insert)

        print(f"Successfully Insert {total_rows} rows to {table_name}!")
        if rows_filtered > 0:
            print(f"   ({rows_filtered} rows are ignore because are not valid ID)")

    except Error as e:
        print(f"Failed to INSERT data to {table_name}: {e}")
        conn.rollback()
    except FileNotFoundError:
        print(f"FILE NOT FOUND: Make sure that file '{csv_file}' is in folder 'scraped_data'.")
        conn.rollback()
    finally:
        cursor.close()


if __name__ == "__main__":
    conn = connect_to_db()
    
    if conn:
        try:
            # Table dim_vehicles
            insert_data(conn, 'dim_vehicles', CSV_FILES['dim_vehicles'])
            
            # Get Valid ID-
            valid_ids = get_existing_leasecar_ids(conn)
            
            # Tabel fact_price 
            insert_data(conn, 'fact_price', CSV_FILES['fact_price'], valid_ids=valid_ids)
            
            # Table dim_color
            insert_data(conn, 'dim_color', CSV_FILES['dim_color'])
            
        finally:
            conn.close()
            print("\nDatabase Connection Closed.")