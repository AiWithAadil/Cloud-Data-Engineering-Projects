import json
import csv
import boto3
import os
from io import StringIO

def lambda_handler(event, context):
    # Initialize S3 client
    s3 = boto3.client('s3')

    # Extract bucket name and key from the event
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    json_key = event['Records'][0]['s3']['object']['key']

    # Download the JSON file from S3
    json_obj = s3.get_object(Bucket=bucket_name, Key=json_key)
    json_data = json_obj['Body'].read().decode('utf-8')

    # Load JSON data
    records = json.loads(json_data)

    # Define the CSV file name
    csv_key = json_key.replace('.json', '_transformed.csv')

    # Transform JSON to CSV
    csv_buffer = StringIO()
    csv_writer = csv.writer(csv_buffer)

    # Write CSV header
    csv_writer.writerow(['title', 'created_utc', 'selftext', 'url'])

    # Write CSV rows
    for record in records:
        csv_writer.writerow([
            record.get('title', ''),
            record.get('created_utc', ''),
            record.get('selftext', ''),
            record.get('url', '')
        ])

    # Upload the transformed CSV file back to S3
    s3.put_object(Bucket=bucket_name, Key=csv_key, Body=csv_buffer.getvalue())

    return {
        'statusCode': 200,
        'body': json.dumps(f'Transformed CSV uploaded to {bucket_name}/{csv_key}')
    }
