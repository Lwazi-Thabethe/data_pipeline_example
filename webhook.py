from flask import Flask, requests
import os
import json
import requests

app = Flask(__name__)

# URL of the deployed producer
PRODUCER_ENDPOINT = os.environ['PRODUCER_ENDPOINT']

@app.route("/webhook", methods=["GET"])
def verify():
    """Dropbox webhook verification"""
    return request.args.get("challenge"), 200

@app.route("/webhook", methods=["POST"])
def webhook_notify():
    """Receive Dropbox notifications and forward to producer"""
    data = request.json
    print("Dropbox update received:", data)

    try:
        resp = requests.post(PRODUCER_ENDPOINT, json=data)
        print(f"Forwarded to producer, status code: {resp.status_code}")
        return "Forwarded to producer", 200
    except Exception as e:
        print(f"Error forwarding to producer: {e}")
        return "Error forwarding", 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
