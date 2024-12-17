import base64
import requests
import boto3
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import logging

# Spotify Authentication - Fetching Token
CLIENT_ID = "***************************"
CLIENT_SECRET = "**********************"

def get_token():
    auth_string = f"{CLIENT_ID}:{CLIENT_SECRET}"
    auth_bytes = auth_string.encode("utf-8")
    auth_base64 = str(base64.b64encode(auth_bytes), "utf-8")

    url = "https://accounts.spotify.com/api/token"
    headers = {
        "Authorization": "Basic " + auth_base64,
        "Content-Type": "application/x-www-form-urlencoded"
    }
    data = {"grant_type": "client_credentials"}
    
    result = requests.post(url, headers=headers, data=data)
    json_result = result.json()
    token = json_result.get("access_token")
    return token

# Spotify API Request - Search Artist
def get_auth_header(token):
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

def search_for_artist(token, artist):
    url = "https://api.spotify.com/v1/search"
    headers = get_auth_header(token)
    query = f"?q={artist}&type=artist&limit=5"  # Search for 5 artists
    query_url = url + query
    result = requests.get(query_url, headers=headers)

    json_result = result.json()
    artists_info = []

    if 'artists' in json_result and 'items' in json_result['artists'] and json_result['artists']['items']:
        for artist_data in json_result['artists']['items']:
            artist_info = {
                "Artist Name": artist_data.get("name"),
                "URI": artist_data.get("uri"),
                "Followers": artist_data['followers'].get('total'),
                "Genres": ", ".join(artist_data.get('genres', [])),
                "Popularity": artist_data.get("popularity"),
                "Image URL": artist_data['images'][0]['url'] if artist_data.get('images') else None
            }
            artists_info.append(artist_info)
    
    return artists_info

# Load Data to S3 using boto3 (with local AWS credentials)
def load_data_to_s3(artists_info):
    # Initialize boto3 S3 client
    s3_client = boto3.client(
        's3',
        aws_access_key_id='*****************',
        aws_secret_access_key='**************************',  # Replace with your AWS Secret Key
        region_name='us-east-1'  # Replace with your AWS region
    )

    bucket_name = 'spotify-ad-yt'
    s3_key = "spotify/artist_data.json"
    
    # Upload the file as JSON
    s3_client.put_object(
        Body=str(artists_info),
        Bucket=bucket_name,
        Key=s3_key
    )
    logging.info(f"File uploaded to s3://{bucket_name}/{s3_key}")

# Define the Airflow DAG
dag = DAG(
    'spotify_data_pipeline',
    description='Extract Spotify Data, Transform & Load into S3',
    schedule_interval='@daily',  # Schedule as per requirement
    start_date=days_ago(1),
    catchup=False,
)

# Task 1: Get Token
get_token_task = PythonOperator(
    task_id='get_token',
    python_callable=get_token,
    dag=dag,
)

# Task 2: Search for artist (get artist data)
search_artist_task = PythonOperator(
    task_id='search_for_artist',
    python_callable=search_for_artist,
    op_args=["{{ task_instance.xcom_pull(task_ids='get_token') }}", "pop"],  # Example search "pop"
    dag=dag,
)

# Task 3: Load data to S3
load_to_s3_task = PythonOperator(
    task_id='load_to_s3',
    python_callable=load_data_to_s3,
    op_args=["{{ task_instance.xcom_pull(task_ids='search_for_artist') }}"],
    dag=dag,
)

# Task dependencies
get_token_task >> search_artist_task >> load_to_s3_task
