import json
import random
import time
import boto3
from datetime import datetime

products = [
    ("Laptop", "Electronics", 65000),
    ("Phone", "Electronics", 30000),
    ("Keyboard", "Accessories", 2500),
    ("Mouse", "Accessories", 1200)
]

cities = ["Vijayawada", "Hyderabad", "Chennai", "Bangalore"]

order_id = 1000

# Connect to Amazon Data Firehose
firehose = boto3.client(
    "firehose",
    region_name="ap-south-1"
)

stream_name = "kinesis-redshift-firehose"

while True:

    product, category, price = random.choice(products)

    data = {
        "order_id": order_id,
        "customer_id": random.randint(500, 1000),
        "product": product,
        "category": category,
        "quantity": random.randint(1, 5),
        "price": price,
        "city": random.choice(cities),
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }

    # Convert data to JSON
    record = json.dumps(data) + "\n"

    # Send data to Firehose
    firehose.put_record(
        DeliveryStreamName=stream_name,
        Record={
            "Data": record.encode("utf-8")
        }
    )

    print("Sent:", data)

    order_id += 1

    time.sleep(1)