import pandas as pd
import requests
import sqlite3
import random

# API config (use your key from Day 7)
api_key = 'e789ad9163819b9a14d962c92b0d0d94'
cities = ['Thessaloniki', 'Glasgow']  # Example from CSV—expand based on data

# Extract CSV (reuse from Day 6)
try:
    # Extract: Load CSV with error handling
    df_employees = pd.read_csv('Employees.csv')  # Assumes in SQL_projects
    df_employees.columns = df_employees.columns.str.lower().str.strip()
    # List of example cities (add more for variety)
    cities = ['Athens', 'Thessaloniki', 'London', 'Milan', 'Brussels', 'Madrid', 'Barcelona', 'Glasgow', 'Berlin', 'Sofia']
    # Add random city column (fixed: use df_employees)
    df_employees['city'] = df_employees.apply(lambda row: random.choice(cities), axis=1)  # Random city per row
    
    # Calculate bonus (10% if salary > avg)
    salary_col = 'current_salary'  # Use variable for flexibility
    avg_salary = df_employees[salary_col].mean()
    df_employees['bonus'] = df_employees[salary_col].apply(lambda x: x * 0.1 if x > avg_salary else 0)
except Exception as e:
    print(f"CSV extract error: {e}")
    df_employees = pd.DataFrame()  # Empty DataFrame to continue safely

# Preview to check (fixed: use df_employees, moved outside except for safety)
if not df_employees.empty:
    print("Preview with new city column:\n", df_employees[['employee_name', 'department', 'city', 'current_salary']].head())

# Extract API for multiple cities
weather_data = []
for city in cities:
    url = f'http://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}&units=metric'
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        weather_data.append({
            'city': city,
            'temp': data['main']['temp'],
            'description': data['weather'][0]['description']
        })
    except Exception as e:
        print(f"API error for {city}: {e}")

df_weather = pd.DataFrame(weather_data)

# Transform: Join on city (assume employee CSV has 'city' column—adjust if different)
if not df_employees.empty and not df_weather.empty:
    df_enriched = pd.merge(df_employees, df_weather, left_on='city', right_on='city', how='left')  # Left join: Keep all employees

    # Calculate temp_adjusted_bonus (fixed: row-wise apply for multi-column access)
    def adjust_bonus(row):
        return row['bonus'] * 1.1 if row['temp'] < 10 else row['bonus']  # Condition per row

    df_enriched['temp_adjusted_bonus'] = df_enriched.apply(adjust_bonus, axis=1)  # axis=1 for row-wise
    df_enriched = df_enriched.dropna(subset=['current_salary'])  # Clean

# Load: To CSV/DB
df_enriched.to_csv('enriched_employees.csv', index=False)
print("CSV loaded! Preview:\n", df_enriched.head())

conn = sqlite3.connect('enriched_etl.db')
df_enriched.to_sql('enriched_employees', conn, if_exists='replace', index=False)
conn.close()
print("DB loaded!")