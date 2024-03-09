import boto3
from confluent_kafka import Producer
import json
import time
from decimal import Decimal

class DecimalEncoder(json.JSONEncoder):
  def default(self, obj):
    if isinstance(obj, Decimal):
      return str(obj)
    return json.JSONEncoder.default(self, obj)
  
# AWS credentials and region
aws_access_key_id = '<>'
aws_secret_access_key = '<>'
aws_region = '<>'

# DynamoDB table details
dynamodb_table_name = 'card_transactions'

# Kafka producer configuration
kafka_config = {
    'bootstrap.servers': 'localhost:9092', 
    'client.id': 'dynamodb-to-kafka-producer',
}

# Kafka topic for card transactions
kafka_topic = 'card_transactions_topic'

# Function to initialize DynamoDB client
def create_dynamodb_client():
    return boto3.resource('dynamodb', aws_access_key_id=aws_access_key_id,
                          aws_secret_access_key=aws_secret_access_key, region_name=aws_region)

# Function to fetch all card transactions from DynamoDB
def fetch_all_card_transactions_from_dynamodb():
    dynamodb = create_dynamodb_client()
    table = dynamodb.Table(dynamodb_table_name)

    # Scan the entire table (Note: Scans can be inefficient for large tables)
    response = table.scan(Limit=5)

    return response.get('Items', [])

# Function to send card transactions to Kafka topic
def send_card_transaction_to_kafka(transaction_data):
    producer = Producer(kafka_config)

    try:
        # Serialize the transaction data to JSON
        serialized_data = json.dumps(transaction_data)
        # Produce the transaction to the Kafka topic
        producer.produce(kafka_topic, key=str(transaction_data['card_id']), value=serialized_data)

        # Wait for any outstanding messages to be delivered and delivery reports received
        producer.flush()
        print(f"Produced transaction: {transaction_data}")

    except Exception as e:
        print(f"Error producing transaction: {e}")

    finally:
        # Close the producer to release resources
        producer.close()

if __name__ == "__main__":
    # Example: Fetch and produce all card transactions from DynamoDB to Kafka
    while True:
        transactions = fetch_all_card_transactions_from_dynamodb()

        for transaction in transactions:
            send_card_transaction_to_kafka(transaction)

        time.sleep(60)  # Introduce a delay between fetching and producing transactions (adjust as needed)
