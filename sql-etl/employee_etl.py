import pandas as pd
import sqlite3

try:
    # Extract: Load CSV with error handling
    df = pd.read_csv('Employees.csv')  # Assumes in SQL_projects
except FileNotFoundError:
    print("Error: Employees.csv not found! Check the file path.")
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
    print("CSV columns:", df.columns.tolist())  # Keep one print for debug (remove duplicates if any)

    df.columns = df.columns.str.lower().str.strip()  # Lowercase & remove spaces

    # Check if required column exists (extra robustness)
    salary_col = 'current_salary'  # Use variable for easy changes
    name_col = 'employee_name'     # Matches your CSV

    if salary_col not in df.columns:
        print(f"Error: Required column '{salary_col}' not found. Available: {df.columns.tolist()}")
    else:
        try:
            # Transform: Clean & calculate (use variables for columns)
            avg_salary = df[salary_col].mean()
            df['bonus'] = df[salary_col].apply(lambda x: x * 0.1 if x > avg_salary else 0)
            df['total_comp'] = df[salary_col] + df['bonus']
            df = df.dropna()  # Drop missing values
        except KeyError as ke:
            print(f"KeyError in transform: {ke} - Check column names match after lowering.")
        except TypeError as te:
            print(f"TypeError in transform: {te} - Ensure '{salary_col}' is numeric (use pd.to_numeric if needed).")
        except Exception as e:
            print(f"Unexpected transform error: {e}")

        try:
            # Load: To CSV (only if transform succeeded)
            df.to_csv('processed_employees.csv', index=False)
            print("CSV loaded! Preview:\n", df[[name_col, salary_col, 'bonus', 'total_comp']].head())  # Use actual columns
        except KeyError as ke:
            print(f"KeyError in preview: {ke} - Check preview columns exist: {df.columns.tolist()}")
        except Exception as e:
            print(f"Unexpected preview error: {e}")

        # Bonus: Load to SQLite
        conn = sqlite3.connect('employee_etl.db')
        df.to_sql('processed_employees', conn, if_exists='replace', index=False)
        conn.close()
        print("DB loaded!")