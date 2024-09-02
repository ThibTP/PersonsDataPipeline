import sqlite3
import pandas as pd
from queries import get_percentage_germany_gmail, get_top_countries_gmail, get_people_over_60_gmail

class LoadView:
    
    def save_to_sqlite(self, df, db_file, table_name):
        """Saving df to a SQLite db"""
        with sqlite3.connect(db_file) as conn:
            df.to_sql(table_name, conn, if_exists='replace', index=False)
            print(f"Data saved to table '{table_name}' in database '{db_file}'.")

    def query_and_view_sqlite(self, db_file, query):
        """Querying db and viewing data with pd"""
        with sqlite3.connect(db_file) as conn:
            df = pd.read_sql_query(query, conn)
            return df

    def get_percentage_germany_gmail(self, conn):
        """Getting percentage of users in Germany using Gmail"""
        query = get_percentage_germany_gmail()
        return pd.read_sql_query(query, conn)

    def get_top_countries_gmail(self, conn):
        """Getting top countries using Gmail"""
        query = get_top_countries_gmail()
        return pd.read_sql_query(query, conn)

    def get_people_over_60_gmail(self, conn):
        """Getting number of people over 60 using Gmail"""
        query = get_people_over_60_gmail()
        return pd.read_sql_query(query, conn)
