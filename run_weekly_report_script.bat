@echo off
CALL C:\Users\user_name\anaconda3\Scripts\activate.bat C:\Users\user_name\anaconda3
CALL conda activate venv1
python C:\Users\user_name\my_file_path\weekly_sales_report_automatic.py > C:\Users\user_name\my_file_path\Logging\output.log 2>&1
