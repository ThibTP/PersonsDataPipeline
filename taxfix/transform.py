import pandas as pd
from datetime import datetime

class DataTransform:
    @staticmethod
    def anonymize_data(df):
        """Anonymizing sensitive user data while keeping all columns"""
        df['firstname'] = '****'
        df['lastname'] = '****'
        df['phone'] = '****'
        df['email'] = df['email'].apply(DataTransform.extract_email_domain)
        df['address_street'] = '****'
        df['address_city'] = '****'
        df['address_zipcode'] = '****'
        df['address_id'] = '****'
        df['address_streetName'] = '****'
        df['address_buildingNumber'] = '****'
        df['address_county_code'] = '****'
        df['address_latitude'] = '****'
        df['address_longitude'] = '****'
        df['birthday'] = df['birthday'].apply(DataTransform.generalize_age_group)
        return df
    
    @staticmethod
    def generalize_age_group(birthday):
        """Converting a birthdate into a 10-year age group"""
        if pd.isna(birthday):
            return 'Unknown'
        try:
            """Ensuring birthday is a string"""
            if not isinstance(birthday, str):
                birthday = str(birthday)
            birth_year = datetime.strptime(birthday, '%Y-%m-%d').year
            current_year = datetime.now().year
            age = current_year - birth_year
            age_group_start = (age // 10) * 10
            age_group_end = age_group_start + 10
            return f'[{age_group_start}-{age_group_end}]'
        except ValueError:
            return 'Unknown'
    
    @staticmethod
    def extract_email_domain(email):
        """Extracting the domain from an email address"""
        return f'****@{email.split("@")[1]}' if pd.notna(email) else '****'
