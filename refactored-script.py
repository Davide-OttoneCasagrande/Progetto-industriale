from dotenv import load_dotenv
import os
import pandas as pd
import requests
import time
import psycopg2
load_dotenv()

# Function to get coordinates using OpenStreetMap Nominatim API
def get_coordinates(comune, provincia="Liguria", nazione="Italy"):
    """Get geographic coordinates for a location using Nominatim API"""
    if pd.notna(comune) and comune != "":
        search_query = f"{comune}, {provincia}, {nazione}"
        url = f"https://nominatim.openstreetmap.org/search?q={search_query}&format=json&limit=1"
        
        try:
            # Add a delay to respect API rate limits
            time.sleep(1)
            response = requests.get(url, headers={"User-Agent": "CommuneCoordinatesFinder/1.0"})
            data = response.json()
            
            if data and len(data) > 0:
                return float(data[0]['lat']), float(data[0]['lon'])
        except Exception as e:
            print(f"Error fetching coordinates for {comune}: {e}")
    
    return None, None

def get_db_connection():
    """Create and return a database connection"""
    return psycopg2.connect(
        dbname=os.getenv('database'),
        user=os.getenv('User'),
        password=os.getenv('password'),
        host=os.getenv('host'),
        port=os.getenv('port')
    )

def fetch_data_from_db(sql_file):
    """Fetch data from database using SQL file"""
    conn = get_db_connection()
    
    with open(sql_file, 'r') as file:
        sql_query = file.read()
    
    cursor = conn.cursor()
    cursor.execute(sql_query)
    rows = cursor.fetchall()
    colnames = [desc[0] for desc in cursor.description]
    
    cursor.close()
    conn.close()
    
    return pd.DataFrame(rows, columns=colnames)

def assign_data(df, filter_condition, id_column, columns_to_assign):
    """Helper function to assign data to dataframe rows matching a condition"""
    for col_name, value in columns_to_assign.items():
        df.loc[filter_condition, col_name] = value
    return df

def process_geographic_hierarchy(df):
    """Process regions, provinces, and communes in the dataframe"""
    # Add columns to the dataframe
    hierarchy_columns = ['Codice Regione', 'Codice Provincia', 'Codice Comune', 
                         'Regione', 'Provincia', 'Comune', 'Latitudine', 'Longitudine']
    for col in hierarchy_columns:
        df[col] = None
    
    # Step 1: Process regions
    searchId = 'ITC'
    comuneCodeLen = 3
    region_rows = df.loc[(df['id'].str.len() == len(searchId) + 1) & (df['id'].str.startswith(searchId))]
    
    # Process each region found
    for _, region_row in region_rows.iterrows():
        region_id = region_row['id']
        region_name = region_row['nome']
        
        # Assign region data
        assign_data(df, df['id'] == region_id, 'id', {
            'Regione': region_name,
            'Codice Regione': region_id
        })
        
        # Step 2: Process provinces for this region
        province_rows = df.loc[(df['id'].str.len() == len(region_id) + 1) & 
                               (df['id'].str.startswith(region_id))]
        
        # Process each province found
        for _, province_row in province_rows.iterrows():
            province_id = province_row['id']
            province_name = province_row['nome']
            
            # Assign province data
            assign_data(df, df['id'] == province_id, 'id', {
                'Provincia': province_name,
                'Codice Provincia': province_id,
                'Codice Regione': region_id
            })
            
            # Step 3: Find communes for this province
            main_com_rows = df.loc[df['nome'] == province_name]
            
            for _, main_com_row in main_com_rows.iterrows():
                commune_id = main_com_row['id']
                
                # Skip if this is the province ID itself
                if commune_id == province_id:
                    continue
                    
                # Extract the first three digits to use as the commune prefix
                if commune_id.isdigit() and len(commune_id) >= comuneCodeLen*2:
                    province_search_id = commune_id[:comuneCodeLen]
                    
                    # Find all communes of this province
                    commune_rows = df.loc[df['id'].str.startswith(province_search_id) & 
                                          (df['id'].str.isdigit())]
                    
                    # Process each commune found
                    for _, commune_row in commune_rows.iterrows():
                        commune_id = commune_row['id']
                        commune_name = commune_row['nome']
                        
                        # Assign commune data
                        assign_data(df, df['id'] == commune_id, 'id', {
                            'Comune': commune_name,
                            'Codice Comune': commune_id,
                            'Codice Provincia': province_id,
                            'Codice Regione': region_id,
                            'Provincia': province_name,
                            'Regione': region_name
                        })
    
    return df

def add_coordinates(df):
    """Add geographic coordinates to communes in the dataframe"""
    for index, row in df.iterrows():
        if pd.notna(row['Comune']) and row['Comune'] != "":
            # Get province name if available
            provincia = "Liguria"
            if pd.notna(row['Provincia']) and row['Provincia'] != "":
                provincia = row['Provincia']
            
            # Get coordinates
            print(f"Getting coordinates for {row['Comune']}")
            lat, lon = get_coordinates(row['Comune'], provincia)
            
            # Update the dataframe
            df.at[index, 'Latitudine'] = lat
            df.at[index, 'Longitudine'] = lon
    
    return df

def fill_missing_coordinates(df):
    """Fill missing coordinates for regions and provinces"""
    missing_coord_rows = df.loc[df['Latitudine'].isnull()]
    
    for idx, row in missing_coord_rows.iterrows():
        if pd.notna(row['Provincia']):
            # Try to find coordinates from the commune with the same name as the province
            matching_commune = df.loc[df['Comune'] == row['Provincia']]
            
            if not matching_commune.empty:
                lat = matching_commune.iloc[0]['Latitudine']
                lon = matching_commune.iloc[0]['Longitudine']
                
                if pd.notna(lat) and pd.notna(lon):
                    df.at[idx, 'Latitudine'] = lat
                    df.at[idx, 'Longitudine'] = lon
    
    return df

def save_to_db(df, table_name):
    """Save dataframe to database"""
    conn = get_db_connection()
    
    try:
        df.to_sql(table_name, conn, if_exists='replace', index=False)
        print(f"Data successfully saved to {table_name} table")
    except Exception as e:
        print(f"Error saving to database: {e}")
    finally:
        conn.close()

def main():
    # Get data from database
    df = fetch_data_from_db('select_location_hierarchy.sql')
    
    # Process geographic hierarchy
    df = process_geographic_hierarchy(df)
    
    # Print summary
    print(f"Processed {df['Codice Regione'].notna().sum()} regions")
    print(f"Processed {df['Codice Provincia'].notna().sum()} provinces")
    print(f"Processed {df['Codice Comune'].notna().sum()} communes")
    
    # Clean up dataframe
    df = df.drop(columns=['nome'])
    df = df.rename(columns={'id': 'Codice ISTAT'})
    
    # Add coordinates
    df = add_coordinates(df)
    
    # Save to CSV
    df.to_csv('gerarchia luogo con coordinate.csv', index=False)
    print("Coordinates added and saved to 'gerarchia luogo con coordinate.csv'")
    
    # Fill missing coordinates
    df = fill_missing_coordinates(df)
    
    # Save to database
    save_to_db(df, "gerarchia_luogo")

if __name__ == "__main__":
    main()
