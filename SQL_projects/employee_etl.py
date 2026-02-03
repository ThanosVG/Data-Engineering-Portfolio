import pandas as pd
import sqlite3

try:
    # Extract: Load CSV with error handling
    df = pd.read_csv('Employees.csv')  # Assumes in SQL_projects
    print("CSV columns:", df.columns.tolist())  # Prints ['Name', 'Age', 'Salary', ...]
except FileNotFoundError:
    print("Error: Employees.csv not found! Check the file path.")
    # Optional: Exit script or use a default empty DF
    df = pd.DataFrame()  # Empty DataFrame to continue safely
except pd.errors.ParserError:
    print("Error: Issue parsing the CSV (e.g., malformed file).")
    df = pd.DataFrame()
except Exception as e:  # Catch-all for unexpected errors
    print(f"Unexpected error: {e}")
    df = pd.DataFrame()
if df.empty:
    print("No data loaded—pipeline stopping.")
    
else:
    print("CSV columns:", df.columns.tolist())  # Keep this for debug
    df.columns = df.columns.str.lower().str.strip()  # Lowercase & remove spaces
    
    required_col = 'current_salary'
    if required_col not in df.columns:
        print(f"Error: Required column '{required_col}' not found after standardization. Available: {df.columns.tolist()}")
    else:
        try:
            # Transform: Clean & calculate (use lowercase now)
            avg_salary = df[required_col].mean()
            df['bonus'] = df[required_col].apply(lambda x: x * 0.1 if x > avg_salary else 0)
            df['total_comp'] = df[required_col] + df['bonus']
            df = df.dropna()  # Drop missing values
        except KeyError as ke:
            print(f"KeyError in transform: {ke} - Check column names match after lowering.")
        except TypeError as te:
            print(f"TypeError in transform: {te} - Ensure '{required_col}' is numeric (use pd.to_numeric if needed).")
        except Exception as e:
            print(f"Unexpected transform error: {e}")
    
    # Now use lowercase: avg_salary = df['salary'].mean()
    # Transform: Clean & calculate (e.g., add 10% bonus if salary > avg)
    avg_salary = df['current_salary'].mean()
    df['bonus'] = df['current_salary'].apply(lambda x: x * 0.1 if x > avg_salary else 0)
    df['total_comp'] = df['current_salary'] + df['bonus']
    df = df.dropna()  # Drop missing values

    # Load: To CSV
    df.to_csv('processed_employees.csv', index=False)
    print("CSV loaded! Preview:\n", df.head())

    # Bonus: Load to SQLite
    conn = sqlite3.connect('employee_etl.db')
    df.to_sql('processed_employees', conn, if_exists='replace', index=False)
    conn.close()
    print("DB loaded!")