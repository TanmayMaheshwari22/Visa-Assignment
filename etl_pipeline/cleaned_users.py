# Import required libraries
import pandas as pd
import yaml
import mysql.connector
import psycopg2
import logging
import re
import urllib.parse  # Required to encode PostgreSQL password in URL

# ---------- CONFIGURATION ----------

with open(r"./config.yaml", "r") as f:
    config = yaml.safe_load(f)

# MySQL connection details
mysql_config=config['mysql']

# PostgreSQL connection details (used for target DB)
postgres_config=config['postgres']

# Source and Target table/database names
SOURCE_TABLE = 'users'
TARGET_TABLE = 'users_cleaned'
target_db = 'target'  # Target PostgreSQL database name
# ----------------------------------


# ---------- LOGGING SETUP ----------
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


# ---------- FUNCTION DEFINITIONS ----------

def read_from_mysql(config, table_name):
    """Reads data from a MySQL table into a pandas DataFrame."""
    logging.info("Connecting to MySQL and reading table: %s", table_name)
    conn = mysql.connector.connect(**config)
    conn.database='dev'
    query = f"SELECT * FROM {table_name}"
    df = pd.read_sql(query, conn)
    conn.close()
    logging.info("Data read from MySQL: %d rows", len(df))
    return df

def cast_types_for_postgres(df):
    """Casts column types to PostgreSQL-compatible formats."""
    logging.info("Casting types for PostgreSQL compatibility...")

    if 'signup_date' in df.columns:
        df['signup_date'] = pd.to_datetime(df['signup_date'], errors='coerce')

    if 'user_id' in df.columns:
        df['user_id'] = pd.to_numeric(df['user_id'], downcast='integer', errors='coerce')

    if 'email' in df.columns:
        df['email'] = df['email'].astype('string')

    if 'country' in df.columns:
        df['country'] = df['country'].astype('string')

    if 'name' in df.columns:
        df['name'] = df['name'].astype('string')

    logging.info("Typecasting completed.")
    return df

def clean_user_data(df):
    """Cleans user data: drops invalid emails and lowercases them."""
    logging.info("Cleaning data...")

    # Drop rows with NULL email
    df = df[df['email'].notnull()]

    # Keeping all the mail ids with '@' so this will remove all the mails wihtout '@'
    df = df[df['email'].str.contains('@', na=False)]

    # Removing all the emails without a domain
    df=df[df['email'].str.strip().str[-1]!='@']

    # Convert emails to lowercase
    df['email'] = df['email'].str.lower()

    logging.info("Data cleaned: %d rows remaining", len(df))
    return df


def create_csv(df):
    df.to_csv('cleaned_users.csv', index=False )

def create_target_database(config ,df, table_name):

    postgres={
          'user':config['user'],
          'password':config['password'],
          'host':config['host'],
          'port':config['port'],
          'database':'target'
    }

    """Creates a target PostgreSQL database if it doesn't exist."""
    conn = psycopg2.connect(**postgres)
    conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = conn.cursor()

    cursor.execute(f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
    user_id INT ,
    name VARCHAR(100),
    email VARCHAR(100),
    signup_date DATE,
    country VARCHAR(50)
    )
                    """)

    print(f"Table {table_name} created.")

    cursor.execute('truncate table users_cleaned')


    logging.info("Loading data into PostgreSQL table: %s", table_name)

    with open('cleaned_users.csv', 'r') as f:
        next(f)  # Skip header
        cursor.copy_expert(f"COPY {table_name} FROM STDIN WITH CSV HEADER", f)


    conn.commit()
    cursor.close()
    conn.close()

    logging.info("Data loaded into PostgreSQL (%s)", table_name)

def main():
    """Main ETL pipeline driver function."""
    logging.info("Starting ETL pipeline: MySQL → Clean → PostgreSQL")

    # Step 1: Read from MySQL
    df_raw = read_from_mysql(mysql_config, SOURCE_TABLE)

    # Step 2: Cast data types
    df_casted = cast_types_for_postgres(df_raw)

    # Step 3: Clean the data
    df_cleaned = clean_user_data(df_casted)

    create_csv(df_cleaned)

    create_target_database(postgres_config,df_cleaned,TARGET_TABLE)

    logging.info("ETL pipeline completed successfully!")


# ---------- SCRIPT ENTRY POINT ----------
if __name__ == '__main__':
    main()


