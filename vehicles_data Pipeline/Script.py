from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import pandas as pd
import boto3
from io import StringIO

# Define the API URL
api_url = "https://data.opendatasoft.com/api/explore/v2.1/catalog/datasets/all-vehicles-model@public/records"
params = {
    "limit": 20  # Number of records to fetch
}

def extract_data(api_url, params):
    """
    Extract data from the API and return it as a DataFrame
    """
    # Send a GET request to the API
    response = requests.get(api_url, params=params)
    response.raise_for_status()  # Raise an error for bad HTTP status codes

    # Parse the JSON response
    data = response.json()
    
    # Extract relevant data from the 'results' key
    records = data.get('results', [])
    
    if not records:
        raise ValueError("No records found in the response.")
    
    # Convert records to a pandas DataFrame
    df = pd.json_normalize(records)
    
    return df

def transform_to_csv(df):
    """
    Transform the DataFrame to CSV format and return as a string
    """
    # Convert the DataFrame to CSV in memory
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    return csv_buffer.getvalue()

def load_to_s3(csv_data, bucket_name, file_name):
    """
    Upload the CSV data to S3
    """
    # Initialize a session using Amazon S3
    s3 = boto3.client('s3')

    # Upload CSV to the specified S3 bucket
    s3.put_object(Bucket=bucket_name, Key=file_name, Body=csv_data)
    print(f"Data uploaded to S3 bucket '{bucket_name}' with filename '{file_name}'")

def etl_task():
    """
    The overall ETL process
    """
    # Extract Data
    df = extract_data(api_url, params)
    
    # Transform Data to CSV
    csv_data = transform_to_csv(df)
    
    # Load Data to S3
    load_to_s3(csv_data, 'your-s3-bucket-name', 'vehicles_data.csv')

# Define the DAG
with DAG(
    'vehicle_data_etl',
    default_args={
        'owner': 'airflow',
        'retries': 1,
    },
    description='ETL DAG to fetch vehicle data and load into S3',
    schedule_interval='@daily',  # Change as per your schedule
    start_date=datetime(2024, 12, 11),  # Start from this date
    catchup=False,
) as dag:

    # Define the Extract, Transform, Load (ETL) tasks
    extract_task = PythonOperator(
        task_id='extract_task',
        python_callable=extract_data,
        op_args=[api_url, params],  # Pass the arguments to the function
    )

    transform_task = PythonOperator(
        task_id='transform_task',
        python_callable=transform_to_csv,
        op_args=['{{ task_instance.xcom_pull(task_ids="extract_task") }}'],  # Pull the output from the extract task
    )

    load_task = PythonOperator(
        task_id='load_task',
        python_callable=load_to_s3,
        op_args=['{{ task_instance.xcom_pull(task_ids="transform_task") }}', 'your-s3-bucket-name', 'vehicles_data.csv'],
    )

    # Set task dependencies
    extract_task >> transform_task >> load_task
 # This task will run when the DAG is triggered
