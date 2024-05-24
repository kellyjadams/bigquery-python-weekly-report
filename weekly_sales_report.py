import os
import pandas as pd
from google.cloud import bigquery

# Initialize BigQuery client to interact with the database
client = bigquery.Client()

# Define start and end date for the report period
start_date = '2024-05-13'
end_date = '2024-05-19'
# Calculate week number for folder naming
week_num = 20

# Format dates to more readable form for folder creation
try:
    start_date_components = start_date.split('-')
    start_date_formatted = f"{int(start_date_components[1]):g}-{int(start_date_components[2]):g}-{start_date_components[0][-2:]}"

    end_date_components = end_date.split('-')
    end_date_formatted = f"{int(end_date_components[1]):g}-{int(end_date_components[2]):g}-{end_date_components[0][-2:]}"

    folder_name = f"{week_num} {start_date_formatted} to {end_date_formatted}"

    # Define path for main report folder and ensure it's created
    main_folder_path = os.path.join(os.getcwd(), folder_name)
    if not os.path.exists(main_folder_path):
        os.makedirs(main_folder_path)
    print(f"Folder created at: {main_folder_path}")
except ValueError as ve:
    print(f"Error processing date formats: {ve}")
except Exception as e:
    print(f"An unexpected error occurred: {e}")

# Define SQL query using parameters for flexibility and security
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
# Configure query parameters to safeguard against SQL injection and provide flexibility
job_config = bigquery.QueryJobConfig(
    query_parameters=[
        bigquery.ScalarQueryParameter("start", "STRING", start_date),
        bigquery.ScalarQueryParameter("end", "STRING", end_date)
    ]
)

try:
    # Execute the query and load results into a pandas DataFrame
    query_job = client.query(query, job_config=job_config)
    df = query_job.to_dataframe()

    # Function to calculate net revenue from purchases and expenses
    def calculate_revenue(row):
        total_purchases = row['total_purchases']
        total_expenses = row['total_expenses']
        if total_purchases is None or total_expenses is None:
            return None
        return total_purchases - total_expenses

    print("Query results:")
    print(df)

    # Setup paths and export results to CSV for further analysis and reporting
    csv_folder_path = os.path.join(main_folder_path, 'CSV')
    if not os.path.exists(csv_folder_path):
        os.makedirs(csv_folder_path)

    csv_file_name = f'Report Tables {start_date_formatted} to {end_date_formatted}.csv'
    csv_file_path = os.path.join(csv_folder_path, csv_file_name)
    df.to_csv(csv_file_path, index=False)

    print(f"CSV file: {csv_file_name} exported successfully to: {csv_file_path}")
except Exception as e:
    print("An error occurred during query execution:", e)
