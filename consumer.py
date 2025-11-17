import os, json
from kafka import KafkaConsumer
import firebase_admin
from firebase_admin import credentials, firestore

# Firebase
cred = credentials.Certificate(json.loads(os.environ['FIREBASE_KEY_JSON']))
firebase_admin.initialize_app(cred)
db = firestore.client()

# Kafka consumer
consumer = KafkaConsumer(
    "universities",
    bootstrap_servers=[os.environ['KAFKA_BOOTSTRAP']],
    security_protocol="SASL_SSL",
    sasl_mechanism="PLAIN",
    sasl_plain_username=os.environ['KAFKA_USER'],
    sasl_plain_password=os.environ['KAFKA_PASSWORD'],
    auto_offset_reset='earliest',
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

print("Listening to Kafka topic 'universities'...")

for msg in consumer:
    uni = msg.value
    uni_id = uni["id"]
    db.collection("universities").document(uni_id).set(uni)
    print(f"Saved {uni['name']} to Firebase!")
