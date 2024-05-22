import os
import pandas as pd
from google.cloud import bigquery

# Initialize BigQuery client
client = bigquery.Client()

# Define start and end date times
start_date = '2024-05-13'  # Assume these are validated to be in the correct format
end_date = '2024-05-19'
# Define the main folder name
week_num = 20  # Define or calculate this as needed

# Parse start_date and end_date into the desired format
try:
    start_date_components = start_date.split('-')
    start_date_formatted = f"{int(start_date_components[1]):g}-{int(start_date_components[2]):g}-{start_date_components[0][-2:]}"

    end_date_components = end_date.split('-')
    end_date_formatted = f"{int(end_date_components[1]):g}-{int(end_date_components[2]):g}-{end_date_components[0][-2:]}"

    folder_name = f"{week_num} {start_date_formatted} to {end_date_formatted}"

    # Define the main folder path
    main_folder_path = os.path.join(os.getcwd(), folder_name)  # More portable across different OS

    # Create the main folder if it doesn't exist
    if not os.path.exists(main_folder_path):
        os.makedirs(main_folder_path)
    print(f"Folder created at: {main_folder_path}")
except ValueError as ve:
    print(f"Error processing date formats: {ve}")
except Exception as e:
    print(f"An unexpected error occurred: {e}")

# Parameterized SQL Query
query = """
WITH transactions AS (
    SELECT 
        DATE(timestamp, 'America/New_York') as purchase_date, 
        SUM(amount_usd) as total_purchases
    FROM 
        `project-id.database.transactions` t
    WHERE 
        DATE(timestamp, 'America/New_York') BETWEEN @start AND @end
    GROUP BY 
        purchase_date
),
expenses as(
    SELECT 
        DATE(timestamp, 'America/New_York') as expense_date, 
        SUM(expenses) as total_expenses
    FROM 
        `project-id.database.expenses` t
    WHERE 
        DATE(timestamp, 'America/New_York') BETWEEN @start AND @end
    GROUP BY expense_date
)
SELECT 
    day, 
    FORMAT_DATE('%a', day) as day_of_the_week, 
    t.total_purchases, 
    e.total_expenses
FROM 
    UNNEST(GENERATE_DATE_ARRAY(DATE(@end), DATE(@start), INTERVAL -1 DAY)) AS day
    LEFT JOIN transactions t ON t.purchase_date = day
    LEFT JOIN expenses e ON e.expense_date = day
WHERE 
    day BETWEEN @start AND @end
ORDER BY 
    day
"""

# Set up query parameters
job_config = bigquery.QueryJobConfig(
    query_parameters=[
        bigquery.ScalarQueryParameter("start", "STRING", start_date),
        bigquery.ScalarQueryParameter("end", "STRING", end_date)
    ]
)

try:
    # Execute the query
    query_job = client.query(query, job_config=job_config)
    df = query_job.to_dataframe()

    # Function to calculate the difference between two columns
    def calculate_revenue(row):
        total_purchases = row['total_purchases']
        total_expenses = row['total_expenses']
        if total_purchases is None or total_expenses is None:
            return None
        return total_purchases - total_expenses

    # Print DataFrame
    print("Query results:")
    print(df)

    # Define the CSV subfolder path
    csv_folder_path = os.path.join(main_folder_path, 'CSV')

    # Create the CSV subfolder if it doesn't exist
    if not os.path.exists(csv_folder_path):
        os.makedirs(csv_folder_path)

    # Define the filename for the CSV file
    csv_file_name = f'Report Tables {start_date_formatted} to {end_date_formatted}.csv'

    # Combine the CSV folder path and filename for the CSV file
    csv_file_path = os.path.join(csv_folder_path, csv_file_name)

    # Export DataFrame to CSV file
    df.to_csv(csv_file_path, index=False)

    print(f"CSV file: {csv_file_name} exported successfully to: {csv_file_path}")

except Exception as e:
    # Handle any exceptions
    print("An error occurred during query execution:", e)
