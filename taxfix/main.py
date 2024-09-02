import os
import config
import pandas as pd
import sqlite3
import json
from extract import DataFetcher
from transform import DataTransform
from load import LoadView

def main():
    base_url = config.base_url
    quantity = config.quantity
    cached_data = '/app/data/cached_data.json'

    """Initializing the DataFetcher with the base URL"""
    fetcher = DataFetcher(base_url)

    """Initializing anonymized_df"""
    anonymized_df = None

    """Checking if the cache file exists and loading data"""
    if os.path.exists(cached_data):
        print("Loading data from cache...")
        with open(cached_data, 'r') as file:
            all_person_data = json.load(file)
        df = pd.DataFrame(all_person_data)
        
        """Expanding address data into separate columns"""
        if 'address' in df.columns:
            address_df = pd.json_normalize(df['address'])
            address_df.columns = [f"address_{col}" if col != 'id' else 'address_id' for col in address_df.columns]
            df = df.drop(columns=['address']).join(address_df)
        
        """Anonymizing the data"""
        anonymized_df = DataTransform.anonymize_data(df)
    
    else:
        print("Fetching data from API...")
        """ Else fetching data with specified parameters"""
        all_person_data = fetcher.fetch_all_data(quantity, cached_data)

        """Checking if data is retrieved"""
        if not all_person_data:
            print("No data retrieved.")
            return

        """Converting to df"""
        df = pd.DataFrame(all_person_data)
        
        """Ensuring the birthday column is in the correct format"""
        if 'birthday' in df.columns:
            df['birthday'] = df['birthday'].astype(str).replace('nan', pd.NA)
        
        """Expanding address data into separate columns"""
        if 'address' in df.columns:
            address_df = pd.json_normalize(df['address'])
            address_df.columns = [f"address_{col}" if col != 'id' else 'address_id' for col in address_df.columns]
            df = df.drop(columns=['address']).join(address_df)
        
        """Anonymizing the data"""
        anonymized_df = DataTransform.anonymize_data(df)
        
        """Saving the anonymized df to the cached file"""
        anonymized_data = anonymized_df.to_dict(orient='records')
        with open(cached_data, 'w') as file:
            json.dump(anonymized_data, file)
        print(f"Anonymized data saved to {cached_data}.")
    
    """Proceeding if anonymized_df has been created"""
    if anonymized_df is not None:
        """Loading data into db"""
        db_file = '/app/data/data.db'
        table_name = 'persons_data'
        loader = LoadView()
        loader.save_to_sqlite(anonymized_df, db_file, table_name)
        
        """Querying data from db"""
        with sqlite3.connect(db_file) as conn:
            percentage_germany_df = loader.get_percentage_germany_gmail(conn)
            top_countries_df = loader.get_top_countries_gmail(conn)
            people_over_60_df = loader.get_people_over_60_gmail(conn)
            
            print("Percentage of Users in Germany Using Gmail:")
            print(percentage_germany_df)
            
            print("\nTop Countries Using Gmail:")
            print(top_countries_df)
            
            print("\nPeople Over 60 Using Gmail:")
            print(people_over_60_df)
    else:
        print("No more data available")

if __name__ == "__main__":
    main()
