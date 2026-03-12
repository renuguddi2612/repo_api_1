import json
import urllib.request
import boto3
import csv
import io
from datetime import datetime

s3 = boto3.client("s3")

BUCKET_NAME = "api2-user-data-bucket"

API_URL = "https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd"

def lambda_handler(event, context):

    req = urllib.request.Request(
        API_URL,
        headers={"User-Agent": "Mozilla/5.0"}
    )

    response = urllib.request.urlopen(req)
    data = json.loads(response.read())

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    json_file = f"crypto_{timestamp}.json"
    csv_file = f"crypto_{timestamp}.csv"

    # Upload JSON
    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=json_file,
        Body=json.dumps(data),
        ContentType="application/json"
    )

    # Convert JSON → CSV
    csv_buffer = io.StringIO()

    fieldnames = data[0].keys()
    writer = csv.DictWriter(csv_buffer, fieldnames=fieldnames)

    writer.writeheader()
    writer.writerows(data)

    # Upload CSV
    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=csv_file,
        Body=csv_buffer.getvalue(),
        ContentType="text/csv"
    )

    return {
        "statusCode": 200,
        "message": "Crypto data uploaded",
        "json_file": json_file,
        "csv_file": csv_file
    }