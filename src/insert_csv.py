import os
import pandas as pd
import psycopg2

# Script per eseguire connessione a un Database PostgreSQL, creare tabelle vuote e riempirle con i dati dei file CSV presenti in una cartella

def connect_db():
    """Crea una connessione al database Postgres."""
    return psycopg2.connect(
        database="postgres",   # Sostituisci con il tuo database
        user="admin",         # Sostituisci con il tuo utente
        password="adminpassword",   # Sostituisci con la tua password
        host="localhost",          # Sostituisci con l'host del tuo database
        port="5432"                # Sostituisci con la port del tuo database
    )

def create_table(cursor, table_name):
    """Crea una tabella nel database per i file CSV specificati."""
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        ID VARCHAR(50) PRIMARY KEY,
        NAME VARCHAR(255)
    );
    """
    cursor.execute(create_table_query)
    print(f"Created table {table_name}")

def load_csv_to_db(cursor, table_name, csv_file_path):
    """Carica un file CSV nella tabella specificata."""
    # Crea la tabella se non esiste
    create_table(cursor, table_name)

    # Carica il file CSV nel database
    with open(csv_file_path, 'r', encoding='utf-8') as f:
        next(f)  # Salta l'intestazione
        cursor.copy_expert(f"COPY {table_name} FROM STDIN WITH (FORMAT CSV, HEADER FALSE)", f)
    print(f"Loaded {csv_file_path} into {table_name}")

def main(directory_path):
    """Carica tutti i file CSV presenti nella directory specificata nel database."""
    conn = connect_db()
    cursor = conn.cursor()

    for filename in os.listdir(directory_path):
        if filename.endswith('.csv'):
            table_name = filename[:-4]  # Rimuovi .csv dal nome del file per ottenere il nome della tabella
            csv_file_path = os.path.join(directory_path, filename)
            try:
                load_csv_to_db(cursor, table_name, csv_file_path)
            except Exception as e:
                print(f"Error loading {csv_file_path}: {e}")

    conn.commit()  # Effettua il commit delle modifiche
    cursor.close()
    conn.close()

if __name__ == "__main__":
    directory_path = 'C:\\Users\\lenov\\Desktop\\Progetto Industriale'  # Sostituisci con il percorso della tua cartella CSV
    main(directory_path)