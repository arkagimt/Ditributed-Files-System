#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import os
import json
from flask import Flask, request
from base64 import b64encode, b64decode

STORAGE_DIR = "storage1"

if not os.path.exists(STORAGE_DIR):
    os.makedirs(STORAGE_DIR)

app = Flask(__name__)

@app.route("/store_file_chunk", methods=["POST"])
def store_file_chunk():
    file_name = request.json["file_name"]
    chunk_id = request.json["chunk_id"]
    file_data = b64decode(request.json["file_data"].encode())

    file_path = os.path.join(STORAGE_DIR, f"{file_name}_chunk{chunk_id}")

    with open(file_path, "wb") as f:
        f.write(file_data)

    return json.dumps({"status": "success"})

@app.route("/retrieve_file_chunk", methods=["POST"])
def retrieve_file_chunk():
    file_name = request.json["file_name"]
    chunk_id = request.json["chunk_id"]

    file_path = os.path.join(STORAGE_DIR, f"{file_name}_chunk{chunk_id}")

    with open(file_path, "rb") as f:
        file_data = f.read()

    return json.dumps({"file_data": b64encode(file_data).decode()})

if __name__ == "__main__":
    app.run(port=6001)

