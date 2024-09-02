import json
import os
import requests
from requests.adapters import HTTPAdapter
from requests.exceptions import HTTPError, RequestException, Timeout, ConnectionError
from urllib3.util.retry import Retry

class DataFetcher:
    def __init__(self, base_url, retries=3):
        """Initializing the DataFetcher with a base URL and a retry policy"""
        self.base_url = base_url
        self.session = self._create_session(retries)

    def _create_session(self, retries):
        """Creating a session with the retries"""
        session = requests.Session()
        retry = Retry(
            total=retries,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session

    def fetch_person_data(self, quantity, gender=None, birthday_start=None):
        """Fetching persons data based on quantity, gender, and birthday_start"""
        params = {
            '_quantity': quantity
        }
        if gender:
            params['_gender'] = gender
        if birthday_start:
            params['_birthday_start'] = birthday_start

        try:
            response = self.session.get(self.base_url, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            """Defining a Data Quality Check"""
            if 'data' in data and isinstance(data['data'], list) and len(data['data']) > 0:
                return data['data']
            else:
                print(f"No data returned for gender: {gender}, quantity: {quantity}, birthday_start: {birthday_start}")
                return []
        
        except (HTTPError, ConnectionError, Timeout, RequestException) as e:
            print(f"Error fetching data for gender {gender}, quantity {quantity}, birthday_start {birthday_start}: {e}")
            return []

    def fetch_all_data(self, total_quantity, cached_data='cached_data.json'):
        """Fetching data and checking if cached data first."""
        cache_dir = os.path.dirname(cached_data)
        if cache_dir and not os.path.exists(cache_dir):
            os.makedirs(cache_dir)

        """Checking if cached data exists"""
        if os.path.exists(cached_data):
            print("Loading data from cache...")
            with open(cached_data, 'r') as file:
                data = json.load(file)
            return data
        
        data = []
        """API max limit per request"""
        batch_size = 1000  
        for i in range(0, total_quantity, batch_size):
            print(f"Fetching batch {i // batch_size + 1}/{(total_quantity + batch_size - 1) // batch_size} with {batch_size} records...")
            batch_data = self.fetch_person_data(batch_size)
            if batch_data:
                data.extend(batch_data)
            else:
                print(f"Batch {i // batch_size + 1} failed to fetch.")
                break  # Stop fetching if a batch fails

        """Saving fetched data to cache"""
        if data:
            with open(cached_data, 'w') as file:
                json.dump(data, file)
            print(f"Fetched and cached {len(data)} records in {cached_data}.")
        else:
            print("No data fetched, nothing cached.")
        
        return data
