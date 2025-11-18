from flask import Flask, request
import os, json, requests
from kafka import KafkaProducer

app = Flask(__name__)

# Kafka
producer = KafkaProducer(
    bootstrap_servers=[os.environ['KAFKA_BOOTSTRAP']],
    security_protocol="SASL_SSL",
    sasl_mechanism="PLAIN",
    sasl_plain_username=os.environ['KAFKA_USER'],
    sasl_plain_password=os.environ['KAFKA_PASSWORD'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

DROPBOX_TOKEN = os.environ['DROPBOX_TOKEN']
KAFKA_TOPIC = os.environ.get('KAFKA_TOPIC', 'example')

def download_dropbox_file(path):
    headers = {
        "Authorization": f"Bearer {DROPBOX_TOKEN}",
        "Dropbox-API-Arg": json.dumps({"path": path})
    }
    r = requests.post("https://content.dropboxapi.com/2/files/download", headers=headers)
    r.raise_for_status()
    return r.content.decode('utf-8')

@app.route("/", methods=["POST"])
def handle_update():
    data = request.json
    # extract changed file path from webhook payload
    file_path = data.get("path_display") 

    csv_text = download_dropbox_file(file_path)
    lines = csv_text.strip().splitlines()
    header = lines[0].split(";")
    values = lines[1].split(";")
    record = dict(zip(header, values))

    producer.send(KAFKA_TOPIC, record)
    producer.flush()
    print(f"Sent record to Kafka: {record}")
    return "OK", 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
