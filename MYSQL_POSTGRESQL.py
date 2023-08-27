import pandas as pd
import psycopg2
import mysql.connector
import json

# Function to read CSV files and migrate data from MySQL to PostgreSQL
def migrate_data(mysql_config, postgres_config):
    # Connect to MySQL and PostgreSQL databases
    mysql_conn = mysql.connector.connect(**mysql_config)
    postgres_conn = psycopg2.connect(**postgres_config)
    
    # Load CSV data into Pandas DataFrame
    mysql_query = "SELECT * FROM your_mysql_table"
    mysql_df = pd.read_sql(mysql_query, mysql_conn)
    
    # Define PostgreSQL schema and table
    postgres_schema = "public"
    postgres_table = "your_postgres_table"
    
    # Migrate data to PostgreSQL
    mysql_df.to_sql(postgres_table, postgres_conn, schema=postgres_schema, if_exists='replace', index=False)
    
    # Close database connections
    mysql_conn.close()
    postgres_conn.close()

# Main function
def main():
    mysql_config = {
        "host": "mysql_host",
        "user": "mysql_user",
        "password": "mysql_password",
        "database": "mysql_database"
    }
    
    postgres_config = {
        "dbname": "postgres_database",
        "user": "postgres_user",
        "password": "postgres_password",
        "host": "postgres_host"
    }
    
    migrate_data(mysql_config, postgres_config)
    
    print("Data migration completed.")

if __name__ == "__main__":
    main()
