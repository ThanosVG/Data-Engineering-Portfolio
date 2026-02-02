import sqlite3
import pandas as pd

try:
    conn = sqlite3.connect('employee.db')  # Or 'SQL_projects/employee.db'
    df = pd.read_sql_query("SELECT * FROM employees WHERE current_salary > (SELECT AVG(current_salary) FROM employees)", conn)
    print("Data preview:\n", df.head())  # Preview
    print(f"Number of high earners: {len(df)}")  # Extra debug
    df.to_csv('high_earners.csv', index=False)  # Export
    conn.close()
except Exception as e:
    print(f"Error: {e}")  # Catches issues like file not found or table missing