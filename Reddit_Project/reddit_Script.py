from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import json
import boto3
import praw

# Reddit API credentials
client_id = ""
client_secret = ""
user_agent = ""

# S3 bucket and file details
S3_BUCKET = "reddit-bucket-ad"
RAW_JSON_FILE = "raw_reddit_data.json"

# Initialize the Reddit client
reddit = praw.Reddit(client_id=client_id, client_secret=client_secret, user_agent=user_agent)

# Function to extract Reddit data
def extract_reddit_data(**kwargs):
    subreddit = kwargs.get('subreddit', 'datascience')
    limit = kwargs.get('limit', 50)
    posts = []
    for submission in reddit.subreddit(subreddit).new(limit=limit):
        posts.append({
            "title": submission.title,
            "created_utc": submission.created_utc,
            "selftext": submission.selftext,
            "url": submission.url
        })
    
    # Save data to a local JSON file
    with open(RAW_JSON_FILE, "w") as json_file:
        json.dump(posts, json_file, indent=4)
    
    print(f"Extracted {len(posts)} posts.")

# Function to upload JSON to S3
def upload_to_s3(**kwargs):
    s3 = boto3.client("s3")
    s3.upload_file(Filename=RAW_JSON_FILE, Bucket=S3_BUCKET, Key=f"raw/{RAW_JSON_FILE}")
    print(f"Uploaded file to S3: s3://{S3_BUCKET}/raw/{RAW_JSON_FILE}")

# Define the DAG
default_args = {
    'owner': 'aadil',
    'depends_on_past': False,
    'retries': 1
}

with DAG(
    dag_id="reddit_to_s3_pipeline",
    default_args=default_args,
    description="Extract Reddit data and load into S3",
    schedule_interval="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:
    
    # Task 1: Extract Reddit Data
    extract_task = PythonOperator(
        task_id="extract_reddit_data",
        python_callable=extract_reddit_data,
        op_kwargs={'subreddit': 'datascience', 'limit': 50}
    )
    
    # Task 2: Upload to S3
    upload_task = PythonOperator(
        task_id="upload_to_s3",
        python_callable=upload_to_s3
    )

    # Set task dependencies
    extract_task >> upload_task
