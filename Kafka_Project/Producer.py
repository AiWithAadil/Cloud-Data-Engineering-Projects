from kafka import KafkaProducer
import time
import json

# Kafka Configuration
KAFKA_TOPIC = "test_topic"
KAFKA_SERVER = "localhost:9092"

# Create Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda x: json.dumps(x).encode('utf-8')  # Serialize to JSON bytes
)

# Read the CSV file
file_path = "./indexProcessed.csv"
try:
    with open(file_path, "r") as file:
        headers = file.readline().strip().split(",")  # Extract headers
        for line in file:
            values = line.strip().split(",")
            message = dict(zip(headers, values))  # Map headers to values
            producer.send(KAFKA_TOPIC, value=message)  # Send JSON message
            print(f"Sent: {message}")
            time.sleep(0.1)  # Optional delay

except FileNotFoundError:
    print(f"Error: File not found at {file_path}")
except Exception as e:
    print(f"An error occurred: {e}")
finally:
    producer.flush()
    producer.close()
