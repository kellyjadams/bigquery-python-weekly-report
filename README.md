# bigquery-python-weekly-report

The Python script I use to automate part of my process for generating a weekly report. 

## Files

### Part 2

New updated Python script that automatically updates the dates based on the current date. Read the article on that [here](). **work in progress**

- [`weekly_sales_report_automatic.py`](weekly_sales_report_automatic.py) - Actual Python script which can be run by itself. 
- [`run_weekly_report_script.bat`](run_weekly_report_script.bat) - A batch job that I use with Windows Task Scheduler to automatically run this script on my local computer. This only works with the `weekly_sales_report_automatic.py` file.

### Part 1
Original python script to automate weekly report. Read the article on that [here](https://www.kellyjadams.com/post/how-i-saved-10-minutes-with-a-python-script).

- [`weekly_sales_report.py`](weekly_sales_report.py)

## Notes

While the code is almost the same as I use, database/table names and query structure have been changed to protect private information. 
