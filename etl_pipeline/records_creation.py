import mysql.connector          # Used to connect and interact with MySQL databases
from faker import Faker         # For generating fake user data
import random                   # To introduce randomness in email flaws
from datetime import datetime   # For working with dates
import yaml
import psycopg2
import logging

# Step 1: Connect to the MySQL Server (not yet selecting a database)
with open(r"./config.yaml", "r") as f:
    config = yaml.safe_load(f)

# MySQL connection details
mysql_config=config['mysql']

# PostgreSQL connection details (used for target DB)
postgres_config=config['postgres']

target_db = 'target'  # Target PostgreSQL database name


def create_source_database(config):

    conn = mysql.connector.connect(**config)
    cursor = conn.cursor()



    # Step 2: Create the database 'dev' if it does not exist
    cursor.execute("CREATE DATABASE IF NOT EXISTS dev")
    print("Database created (or already exists).")

    # Step 3: Select 'dev' as the active database
    conn.database = "dev"

    # Step 4: Create a table named 'users' if it doesn't already exist
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS users (
        user_id INT,
        name VARCHAR(100),
        email VARCHAR(100),
        signup_date DATE,
        country VARCHAR(50)
    )
        """)
    print("Table `users` created.")

    # Step 5: Initialize Faker for generating fake names, emails, dates, etc.
    fake = Faker()

    # List of countries to randomly choose from . (The countries are taken randomly)
    countries = ['USA', 'India', 'UK', 'Germany', 'Japan', 'France', 'Canada']

    # Step 6: Function to generate emails with a mix of flaws
    def generate_email():
        r = random.random()
        base_email = fake.email()

        if r < 0.1:
            return None               # 10% emails are NULL
        elif r < 0.2:
            return base_email.replace('@', '')  # 10% missing '@'
        elif r < 0.3:
            return base_email.split('@')[0] + str('@')   # 10% missing domain
        elif r < 0.4:
            return base_email.upper()           # 10% uppercased
        else:
            return base_email                   # 60% valid

    # Step 7: Generate and insert 100 fake user records into the users table
    for user_id in range(1, 1001):
        name = fake.name()
        email = generate_email()
        signup_date = fake.date_between(start_date='-2y', end_date='today')  # Random date in past 2 years
        country = random.choice(countries)  # Randomly pick a country

        cursor.execute('''
            INSERT INTO users (user_id, name, email, signup_date, country)
            VALUES (%s, %s, %s, %s, %s)
            ''', (user_id, name, email, signup_date, country))

    # Step 8: Commit the changes to the database
    conn.commit()

    # Step 9: Close the connection
    conn.close()

    print("Inserted 100 users with a mix of valid and invalid emails.")



def create_target_database(config, target_db):
    """Creates a target PostgreSQL database if it doesn't exist."""
    logging.info("Creating PostgreSQL target database (if not exists): %s", target_db)
    conn = psycopg2.connect(**config)
    conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = conn.cursor()

    # CREATE DATABASE if it doesn't exist
    cursor.execute(f"SELECT 1 FROM pg_database WHERE datname = '{target_db}'")
    exists = cursor.fetchone()

    if not exists:
        cursor.execute(f"CREATE DATABASE {target_db}")
        logging.info("PostgreSQL database '%s' created.", target_db)
    else:
        logging.info("PostgreSQL database '%s' already exists.", target_db)

    cursor.close()
    conn.close()

def main():

        create_source_database(mysql_config)
        create_target_database(postgres_config, target_db)

# ---------- SCRIPT ENTRY POINT ----------
if __name__ == '__main__':
    main()

