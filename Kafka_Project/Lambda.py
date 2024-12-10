import json
import csv
import boto3
from io import StringIO

# S3 client initialization
s3_client = boto3.client('s3')

# S3 bucket and folder details
SOURCE_BUCKET = 'kafka-bucket-ad'  # Replace with your source bucket name
DEST_BUCKET = 'kafka-bucket-ad'  # Replace with your destination bucket name
DEST_FILE_NAME = 'Transform/merged_stock_market_data.csv'  # Name for the final CSV file

def lambda_handler(event, context):
    try:
        # List all files in the source bucket
        response = s3_client.list_objects_v2(Bucket=SOURCE_BUCKET)
        if 'Contents' not in response:
            print("No files found in the source bucket.")
            return

        # Initialize a list to hold all rows
        data_rows = []

        # Process each file in the source bucket
        for obj in response['Contents']:
            file_key = obj['Key']
            
            # Skip non-CSV files
            if not file_key.endswith('.csv'):
                continue
            
            # Read the CSV file from S3
            file_obj = s3_client.get_object(Bucket=SOURCE_BUCKET, Key=file_key)
            file_content = file_obj['Body'].read().decode('utf-8')

            print(f"Processing file: {file_key}")

            # Process each line in the file as a JSON object
            for line in file_content.splitlines():
                try:
                    # Parse JSON from the line
                    row = json.loads(line)
                    
                    # Add the row to the list with the desired fields
                    data_rows.append({
                        "Index": row.get("Index", ""),
                        "Date": row.get("Date", ""),
                        "Open": row.get("Open", ""),
                        "High": row.get("High", ""),
                        "Low": row.get("Low", ""),
                        "Close": row.get("Close", ""),
                        "Adj Close": row.get("Adj Close", ""),
                        "Volume": row.get("Volume", ""),
                        "CloseUSD": row.get("CloseUSD", "")
                    })
                except json.JSONDecodeError:
                    print(f"Error decoding JSON in line: {line}")
                    continue

        # If no valid rows found, exit early
        if not data_rows:
            print("No valid data rows found.")
            return

        # Write the merged data to a CSV
        csv_buffer = StringIO()
        csv_writer = csv.DictWriter(csv_buffer, fieldnames=[
            "Index", "Date", "Open", "High", "Low", "Close", "Adj Close", "Volume", "CloseUSD"
        ])
        csv_writer.writeheader()
        csv_writer.writerows(data_rows)

        # Upload the CSV to the destination bucket
        csv_buffer.seek(0)
        s3_client.put_object(Bucket=DEST_BUCKET, Key=DEST_FILE_NAME, Body=csv_buffer.getvalue())

        print(f"Merged CSV file uploaded successfully to {DEST_BUCKET}/{DEST_FILE_NAME}")

    except Exception as e:
        print(f"Error: {str(e)}")
