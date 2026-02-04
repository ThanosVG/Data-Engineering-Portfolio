import requests
import pandas as pd
import sqlite3
import time  # New: For retry delay

# API config (replace with your key)
api_key = 'e789ad9163819b9a14d962c92b0d0d94'  # Free from openweathermap.org
city = 'Thessaloniki'  # Test city
url = f'http://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}&units=metric'


data = {}
retries = 3  # Try up to 3 times
for attempt in range(retries):
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        break  # Success—exit loop
    except requests.exceptions.HTTPError as he:
        print(f"HTTP error on attempt {attempt+1}: {he}")
    except Exception as e:
        print(f"Unexpected API error on attempt {attempt+1}: {e}")
    time.sleep(2)  # Wait 2 seconds before retry

if not data:
    print("No data extracted after retries—pipeline stopping.")
else:
    # Transform and load as before...
    # Transform: Flatten JSON to DataFrame, add calcs
    df = pd.json_normalize(data)  # Converts JSON to table

    # Extract nested weather description (since 'weather' is a list of dicts)
    df['weather_description'] = df['weather'].apply(lambda x: x[0]['description'] if x and isinstance(x, list) and len(x) > 0 else 'N/A')

    # Calculate temp_diff (use correct flattened columns)
    df['temp_diff'] = df['main.feels_like'] - df['main.temp']

    try:
        # Select columns (use extracted 'weather_description')
        df = df[['name', 'main.temp', 'main.feels_like', 'temp_diff', 'weather_description']]
    except KeyError as ke:
        print(f"KeyError in select: {ke} - Check API response columns: {df.columns.tolist()}")
        # Optional: Continue with all columns if select fails
        print("Using all columns as fallback.")

    # Load: To CSV
    df.to_csv('processed_weather.csv', index=False)
    print("CSV loaded! Preview:\n", df.head())

    # Bonus: Load to SQLite
    conn = sqlite3.connect('weather_etl.db')
    df.to_sql('weather_data', conn, if_exists='replace', index=False)
    conn.close()
    print("DB loaded!")