from flask import Flask, request
import requests
import os
import json

app = Flask(__name__)

PRODUCER_ENDPOINT = os.environ['PRODUCER_ENDPOINT'] 

@app.route("/webhook", methods=["GET"])
def verify():
    # Dropbox verification
    return request.args.get("challenge"), 200

@app.route("/webhook", methods=["POST"])
def webhook_notify():
    data = request.json
    print("Dropbox update received:", data)

    # Forward to producer
    requests.post(PRODUCER_ENDPOINT, json=data)
    return "", 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
