def get_percentage_germany_gmail():
    """Auery to get the percentage of users in Germany using Gmail"""
    return """

    SELECT 
    (COUNT(CASE WHEN email LIKE '%gmail.com' THEN 1 END) * 100.0 / COUNT(*)) AS percentage_germany_gmail
    FROM 
    persons_data
    WHERE 
    address_country = 'Germany';

    """

def get_top_countries_gmail():
    """Query to get the top countries using Gmail"""
    return """

    SELECT 
    address_country,
    COUNT(*) AS gmail_count
    FROM 
    persons_data
    WHERE 
    email LIKE '%gmail.com'
    GROUP BY 
    address_country
    ORDER BY 
    gmail_count DESC
    LIMIT 3;
    
    """

def get_people_over_60_gmail():
    """Query to get the number of people over 60 years using Gmail"""
    return """

    SELECT 
    COUNT(*) AS count_over_60_gmail
    FROM 
    persons_data
    WHERE 
    email LIKE '%gmail.com' 
    AND (strftime('%Y', 'now') - CAST(SUBSTR(birthday, 1, 4) AS INTEGER)) > 60;

    """


