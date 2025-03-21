from dotenv import load_dotenv
import os
import pandas as pd
import requests
import time
import psycopg2

load_dotenv()


def get_db_connection() -> psycopg2.extensions.connection:
    """Create and return a database connection"""
    return psycopg2.connect(
        dbname = os.getenv('database'),
        user =  os.getenv('User'),
        password = os.getenv('password'),
        host = os.getenv('host'),
        port = os.getenv('port')
    )

def get_connection_string() -> str:
    """Create a connection string."""
    return f"postgresql://{os.getenv('User')}:{os.getenv('password')}@{os.getenv('host')}:{os.getenv('port')}/{os.getenv('database')}"

def fetch_data_from_db(sql_file: str) -> pd.DataFrame:
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

def process_geographic_hierarchy(df: pd.DataFrame, searchId: str) -> pd.DataFrame:
    # Add columns to the dataframe
    df['parent_ID'] = None
    df['Latitudine'] = None
    df['Longitudine'] = None
    
    # Create a mapping of province IDs to their numeric codes
    province_code_mapping = {}
    
    # Step 1: Process regions
    region_rows = df.loc[(df['id'].str.len() == len(searchId) + 1) & 
                         (df['id'].str.startswith(searchId))]
    
    for _, region_row in region_rows.iterrows():
        region_id = region_row['id']
        
        # Step 2: Process provinces
        province_rows = df.loc[(df['id'].str.len() == len(region_id) + 1) & 
                              (df['id'].str.startswith(region_id))]
        
        for _, province_row in province_rows.iterrows():
            province_id = province_row['id']
            
            # Set parent_ID for province
            assign_data(df, df['id'] == province_id, 'id', {
                'parent_ID': region_id
            })
            
            # Find a commune that belongs to this province to get its code
            province_name = province_row['nome']
            sample_communes = df.loc[(df['nome'] == province_name) & 
                                    (df['id'].str.isdigit()) & 
                                    (df['id'].str.len() == 6)]
            
            if not sample_communes.empty:
                # Get the first 3 digits of the commune code
                province_code = sample_communes.iloc[0]['id'][:3]
                province_code_mapping[province_id] = province_code
                
                # Find all communes with this province code
                commune_rows = df.loc[df['id'].str.startswith(province_code) & 
                                     (df['id'].str.isdigit()) &
                                     (df['id'].str.len() == 6)]
                
                # Set parent_ID for communes
                assign_data(df, df['id'].isin(commune_rows['id']), 'id', {
                    'parent_ID': province_id
                })
    #display(df)
    return df


def assign_data(df: pd.DataFrame, filter_condition: bool, id_column: str, columns_to_assign: dict) -> pd.DataFrame:
    """Helper function to assign data to dataframe rows matching a condition"""
    for col_name, value in columns_to_assign.items():
        df.loc[filter_condition, col_name] = value
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
            # Get coordinates
            #print(f"Getting coordinates for {row['Comune']}")  # Uncomment to see progress
            lat, lon = get_coordinates(row['nome'])
            # Update the dataframe
            df.at[index, 'Latitudine'] = lat
            df.at[index, 'Longitudine'] = lon
    return df

def main(pathSQL_request: str) -> None:
    searchId = 'ITC' # Starting search string
    df = fetch_data_from_db(pathSQL_request)
    df = process_geographic_hierarchy(df,searchId)
    
    print(f"Processed {df['parent_ID'].notna().sum()} entry")   # Print summary
    
    # Clean up dataframe
    df = add_coordinates(df)

    print(f"Processed {df['Latitudine'].notna().sum()} coordinates")    # Print summary
    
    # Save to CSV
    #df.to_csv('csv//gerarchia luogo con coordinate.csv', index=False)
    #print("Coordinates added and saved to 'gerarchia luogo con coordinate.csv'")
        
    # Save to database
    save_to_db(df, "gerarchia_luogo")




#Executable
if __name__ == "__main__":
    pathSQL_request = 'select_location_hierarchy.sql'
    main(pathSQL_request)