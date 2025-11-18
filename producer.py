from flask import Flask, request
import os, json, requests
from kafka import KafkaProducer

app = Flask(__name__)

DROPBOX_TOKEN = os.environ['DROPBOX_TOKEN']

producer = KafkaProducer(
    bootstrap_servers=[os.environ['KAFKA_BOOTSTRAP']],
    security_protocol="SASL_SSL",
    sasl_mechanism="PLAIN",
    sasl_plain_username=os.environ['KAFKA_USER'],
    sasl_plain_password=os.environ['KAFKA_PASSWORD'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

KAFKA_TOPIC = os.environ.get('KAFKA_TOPIC', 'example')

def download_dropbox_file(path):
    """Download a file from Dropbox"""
    headers = {
        "Authorization": f"Bearer {DROPBOX_TOKEN}",
        "Dropbox-API-Arg": json.dumps({"path": path})
    }
    r = requests.post("https://content.dropboxapi.com/2/files/download", headers=headers)
    r.raise_for_status()
    return r.content.decode('utf-8')

def send_to_kafka(record):
    """Send a single record to Kafka"""
    try:
        producer.send(KAFKA_TOPIC, record)
        producer.flush()
        print(f"Sent record to Kafka: {record}")
    except Exception as e:
        print(f"Error sending to Kafka: {e}")


@app.route("/process", methods=["POST"])
def handle_update():
    """Handle forwarded Dropbox webhook data"""
    data = request.json
    file_path = data.get("path_display")

    if not file_path or file_path.endswith("/"):
        return "Skipped folder or missing path", 200

    try:
        csv_text = download_dropbox_file(file_path)
        lines = csv_text.strip().splitlines()
        header = lines[0].split(";")

        for line in lines[1:]:
            values = line.split(";")
            record = dict(zip(header, values))
            send_to_kafka(record)

        return f"Sent {len(lines)-1} records", 200

    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        return "Error", 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10001)
