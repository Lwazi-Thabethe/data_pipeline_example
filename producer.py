import os, json, requests
from kafka import KafkaProducer

from dotenv import load_dotenv
load_dotenv()


# Env variables
DROPBOX_TOKEN = os.environ['DROPBOX_TOKEN']
KAFKA_BOOTSTRAP = os.environ['KAFKA_BOOTSTRAP']
KAFKA_USER = os.environ['KAFKA_USER']
KAFKA_PASSWORD = os.environ['KAFKA_PASSWORD']
KAFKA_TOPIC = os.environ.get('KAFKA_TOPIC', 'universities')

# Kafka producer
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BOOTSTRAP],
    security_protocol="SASL_SSL",
    sasl_mechanism="PLAIN",
    sasl_plain_username=KAFKA_USER,
    sasl_plain_password=KAFKA_PASSWORD,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Example: fetch a file from Dropbox
def download_dropbox_file(path):
    headers = {
        "Authorization": f"Bearer {DROPBOX_TOKEN}",
        "Dropbox-API-Arg": json.dumps({"path": path})
    }
    r = requests.post("https://content.dropboxapi.com/2/files/download", headers=headers)
    r.raise_for_status()
    return r.content.decode('utf-8')

# Test file path
file_path = "/university_files/test_university.csv"
csv_text = download_dropbox_file(file_path)

# Convert CSV to dict (for one record example)
lines = csv_text.strip().splitlines()
header = lines[0].split(";")
values = lines[1].split(";")
record = dict(zip(header, values))

# Send to Kafka
producer.send(KAFKA_TOPIC, record)
producer.flush()
print(f"Sent record to Kafka: {record}")
