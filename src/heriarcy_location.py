from dotenv import load_dotenv
import os
import pandas as pd
import requests
import time
import psycopg2
load_dotenv()

# Function to get coordinates using OpenStreetMap Nominatim API
def get_coordinates(comune, regione="Liguria", nazione="Italy"):
    # Format the search query
    if pd.notna(comune) and comune != "":
        search_query = f"{comune}, {regione}, {nazione}"
        
        # Use OpenStreetMap Nominatim API
        url = f"https://nominatim.openstreetmap.org/search?q={search_query}&format=json&limit=1"
        
        try:
            # Add a delay to respect API rate limits
            time.sleep(1)
            response = requests.get(url, headers={"User-Agent": "CommuneCoordinatesFinder/1.0"})
            data = response.json()
            
            if data and len(data) > 0:
                lat = data[0]['lat']
                lon = data[0]['lon']
                return float(lat), float(lon)
        except Exception as e:
            print(f"Error fetching coordinates for {comune}: {e}")
    
    return None, None

conn = psycopg2.connect(
    dbname = os.getenv('database'),
    user = os.getenv('User'),
    password = os.getenv('password'),
    host = os.getenv('host'),
    port = os.getenv('port')
)

with open('select_location_hierarchy.sql', 'r') as file:    # Read the SQL file
    sql_query = file.read()

cursor = conn.cursor()
cursor.execute(sql_query)
rows = cursor.fetchall()    # Fetch all rows from the executed query
colnames = [desc[0] for desc in cursor.description] # Get column names from the cursor

# Close the cursor and connection
cursor.close()
conn.close()

Df = pd.DataFrame(rows, columns=colnames)   # Create a Pandas DataFrame from the fetched data

# Regions: ITC + 1 character (e.g., ITC3)
# Provinces: Region code + 1 character (e.g., ITC3)
# Communes: Numbers found by searching province's name, first three numbers indicate province

# Add columns to the dataframe
Df['Codice Regione'] = None
Df['Codice Provincia'] = None
Df['Codice Comune'] = None
Df['Regione'] = None
Df['Provincia'] = None
Df['Comune'] = None
Df['Latitudine'] = None
Df['Longitudine'] = None

# Step 1: Process regions
searchId = 'ITC'  # Starting search string
comuneCodeLen = 3
region_rows = Df.loc[(Df['id'].str.len() == len(searchId) + 1) & (Df['id'].str.startswith(searchId))]

# Process each region found
for idx, region_row in region_rows.iterrows():
    region_id = region_row['id']
    region_name = region_row['nome']
    
    # Assign region data to all rows with this region ID
    Df.loc[Df['id'] == region_id, 'Regione'] = region_name
    Df.loc[Df['id'] == region_id, 'Codice Regione'] = region_id
    
    # Step 2: Process provinces for this region
    province_rows = Df.loc[(Df['id'].str.len() == len(region_id) + 1) & (Df['id'].str.startswith(region_id))]
    
    # Process each province found
    for p_idx, province_row in province_rows.iterrows():
        province_id = province_row['id']
        province_name = province_row['nome']
        
        # Assign province data to rows with this province ID
        Df.loc[Df['id'] == province_id, 'Provincia'] = province_name
        Df.loc[Df['id'] == province_id, 'Codice Provincia'] = province_id
        Df.loc[Df['id'] == province_id, 'Codice Regione'] = region_id
               
        # Step 3: Find communes for this province by searching for province name
        # Look for communes that match the province name
        main_com_rows = Df.loc[Df['nome'] == province_name]
        
        for mc_idx, main_com_row in main_com_rows.iterrows():
            commune_id = main_com_row['id']
            
            # Skip if this is the province ID itself
            if commune_id == province_id:
                continue
                
            # Extract the first three digits to use as the commune prefix
            if commune_id.isdigit() and len(commune_id) >= comuneCodeLen*2:
                province_search_id = commune_id[:comuneCodeLen]
                
                # Now find all communes of this province (Must start with the code found above)
                commune_rows = Df.loc[Df['id'].str.startswith(province_search_id) & (Df['id'].str.isdigit())]
                
                # For each commune found
                for c_idx, commune_row in commune_rows.iterrows():
                    commune_id = commune_row['id']
                    commune_name = commune_row['nome']
                    
                    # Assign commune data
                    Df.loc[Df['id'] == commune_id, 'Comune'] = commune_name
                    Df.loc[Df['id'] == commune_id, 'Codice Comune'] = commune_id
                    Df.loc[Df['id'] == commune_id, 'Codice Provincia'] = province_id
                    Df.loc[Df['id'] == commune_id, 'Codice Regione'] = region_id
                    Df.loc[Df['id'] == commune_id, 'Provincia'] = province_name
                    Df.loc[Df['id'] == commune_id, 'Regione'] = region_name

# Print summary of how many entries were processed
print(f"Processed {Df['Codice Regione'].notna().sum()} regions")
print(f"Processed {Df['Codice Provincia'].notna().sum()} provinces")
print(f"Processed {Df['Codice Comune'].notna().sum()} communes")

Df = Df.drop(columns=['nome'])  # drop the original 'name' columns
Df = Df.rename(columns={'id': 'Codice ISTAT'}) # chenge the original 'id' columns name to a clearer format

# Process only rows with 'comune' names
for index, row in Df.iterrows():
    if pd.notna(row['Comune']) and row['Comune'] != "":
        # Get province name if available
        provincia = "Liguria"
        if pd.notna(row['Provincia']) and row['Provincia'] != "":
            provincia = row['Provincia']
        
        # Get coordinates
        print(f"Getting coordinates for {row['Comune']}")
        lat, lon = get_coordinates(row['Comune'], provincia)
        
        # Update the dataframe
        Df.at[index, 'Latitudine'] = lat
        Df.at[index, 'Longitudine'] = lon

# Save the updated dataframe to a new CSV file
Df.to_csv('gerarchia luogo con coordinate.csv', index=False)
print("Coordinates added and saved to 'gerarchia luogo con coordinate.csv'")

#found missing coordinates (regions and provinces)
missingCoord_row = Df.loc[Df['Latitudine'].isnull()]

# Process each row found
for cox, missingCoord_row in missingCoord_row.iterrows():
    missCoord_id = missingCoord_row['Codice ISTAT']
    missCoord_name = missingCoord_row['Provincia']

    if missCoord_name:
        # Assign region data to all rows with this region ID
        lat = Df.loc[Df['Comune'] == missCoord_name, 'Latitudine']
        lon = Df.loc[Df['Comune'] == missCoord_name, 'Longitudine']
        Df.loc[Df['Codice ISTAT'] == missCoord_id, 'Latitude'] = lat
        Df.loc[Df['Codice ISTAT'] == missCoord_id, 'Longitudine'] = lat

Df.to_sql("gerarchia_luogo", conn.tostring(), if_exists='replace', index=False)