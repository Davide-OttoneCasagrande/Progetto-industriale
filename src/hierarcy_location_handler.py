from dotenv import load_dotenv
from os import getenv
import pandas as pd
import requests
import time
import psycopg2

load_dotenv()


def get_db_connection() -> psycopg2.extensions.connection:
    """Create and return a database connection"""
    return psycopg2.connect(
        dbname = getenv('database'),
        user = getenv('User'),
        password = getenv('password'),
        host = getenv('host'),
        port = getenv('port')
    )

def get_connection_string() -> str:
    """Create a connection string."""
    return f"postgresql://{getenv('user')}:{getenv('password')}@{getenv('host')}:{getenv('port')}/{getenv('database')}"

def fetch_data_from_db(sql_file) -> pd.DataFrame:
    """Fetch data from database using SQL file"""
    conn = get_db_connection()
    
    with open(sql_file, 'r') as file:   # Read the SQL file
        sql_query = file.read()
    
    cursor = conn.cursor()
    cursor.execute(sql_query)
    rows = cursor.fetchall()    # Fetch all rows from the executed query
    colnames = [desc[0] for desc in cursor.description] # Get column names from the cursor
    
    # Close the cursor and connection
    cursor.close()
    conn.close()
    
    return pd.DataFrame(rows, columns=colnames) # Create a Pandas DataFrame from the fetched data

def save_to_db(df: pd.DataFrame, table_name: str) -> None:
    """Save dataframe to database"""
    conn = get_db_connection()
    
    try:
        df.to_sql(table_name, get_connection_string(), if_exists='replace', index=False)
        print(f"Data successfully saved to {table_name} table")
    except Exception as e:
        print(f"Error saving to database: {e}")
    finally:
        conn.close()


def assign_data(df: pd.DataFrame, filter_condition: bool, id_column: str, columns_to_assign: dict) -> pd.DataFrame:
    """Helper function to assign data to dataframe rows matching a condition"""
    for col_name, value in columns_to_assign.items():
        df.loc[filter_condition, col_name] = value
    return df

def process_geographic_hierarchy(df: pd.DataFrame, searchId: str) -> pd.DataFrame:
    """
    searchid = # Starting search string (macroregion id ex: ITC For north West Italy)
    Process regions, provinces, and communes in the dataframe
    Regions: ITC + 1 character (e.g., ITC3)
    Provinces: Region code + 1 character (e.g., ITC3)
    Communes: Numbers found by searching province's name, first three numbers indicate province
    """
    # Add columns to the dataframe
    hierarchy_columns = ['Codice Regione', 'Codice Provincia', 'Codice Comune', 
                         'Regione', 'Provincia', 'Comune', 'Latitudine', 'Longitudine']
    for col in hierarchy_columns:
        df[col] = None
    
    # Step 1: Process regions
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
            
            # Step 3: Find communes for this province by searching for province name
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

# Function to get coordinates using OpenStreetMap Nominatim API
def get_coordinates(comune: str, provincia: str="Liguria", nazione: str="Italy") -> tuple[float, float]:
    """Get geographic coordinates for a location using Nominatim API"""
    if pd.notna(comune) and comune != "":
        search_query = f"{comune}, {provincia}, {nazione}"  # Format the search query
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

def add_coordinates(df: pd.DataFrame) -> pd.DataFrame:
    """Add geographic coordinates to communes in the dataframe"""
    for index, row in df.iterrows():
        if pd.notna(row['Comune']) and row['Comune'] != "":
            # Get province name if available
            provincia = row['Regione']
            if pd.notna(row['Provincia']) and row['Provincia'] != "":
                provincia = row['Provincia']
            
            # Get coordinates
            #print(f"Getting coordinates for {row['Comune']}")  # Uncomment to see progress
            lat, lon = get_coordinates(row['Comune'], provincia)
            
            # Update the dataframe
            df.at[index, 'Latitudine'] = lat
            df.at[index, 'Longitudine'] = lon

    return df

def fill_missing_coordinates(df: pd.DataFrame) -> pd.DataFrame:
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


def main(sqlRequestPath: str) -> None:
    searchId = 'ITC' # Starting search string
    df = fetch_data_from_db(sqlRequestPath)
    df = process_geographic_hierarchy(df,searchId)
    
    # Print summary
    print(f"Processed {df['Codice Regione'].notna().sum()} regions")
    print(f"Processed {df['Codice Provincia'].notna().sum()} provinces")
    print(f"Processed {df['Codice Comune'].notna().sum()} communes")
    
    # Clean up dataframe
    df = df.drop(columns=['nome'])
    df = df.rename(columns={'id': 'Codice ISTAT'})
    df = add_coordinates(df)
    df = fill_missing_coordinates(df)

    # Print summary
    print(f"Processed {df['Latitudine'].notna().sum()} coordinates")
    
    # Save to CSV
    #df.to_csv('src//csv//gerarchia luogo con coordinate.csv', index=False)
    #print("Coordinates added and saved to 'gerarchia luogo con coordinate.csv'")
        
    # Save to database
    save_to_db(df, "gerarchia_luogo")


    if __name__ == "__main__":
        sqlRequestPath = 'Progetto-industriale\src\script_sql\select_location_hierarchy.sql'
        main(sqlRequestPath)