#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import os
import sys
import json
import requests
from base64 import b64encode, b64decode

CHUNK_SIZE = 1024 * 1024
CENTRAL_SERVER_URL = "http://localhost:5000"

def upload_file(file_path):
    file_name = os.path.basename(file_path)
    file_size = os.path.getsize(file_path)
    total_chunks = (file_size + CHUNK_SIZE - 1) // CHUNK_SIZE

    file_chunks = {}
    with open(file_path, "rb") as f:
        for chunk_id in range(total_chunks):
            chunk_data = f.read(CHUNK_SIZE)
            encoded_chunk_data = b64encode(chunk_data).decode()

            data_node_ids = [1, 2, 3]  # You can change this to any other combination of data node ids.
            file_chunks[str(chunk_id)] = data_node_ids

            print(f"Uploading chunk {chunk_id} to data nodes {data_node_ids}")

            for data_node_id in data_node_ids:
                data_node_url = requests.get(f"{CENTRAL_SERVER_URL}/get_data_node_url", params={"id": data_node_id}).text
                data_node_url += "/store_file_chunk"
                response = requests.post(
                    data_node_url,
                    json={
                        "file_name": file_name,
                        "chunk_id": chunk_id,
                        "file_data": encoded_chunk_data,
                    },
                )

                if response.status_code != 200:
                    print(f"Failed to upload chunk {chunk_id} to data node {data_node_id}")
                    return

    response = requests.post(
        f"{CENTRAL_SERVER_URL}/upload_file",
        json={
            "file_name": file_name,
            "file_chunks": file_chunks,
        },
    )

    print(response.json())

def download_file(file_name, output_path):
    response = requests.post(
        f"{CENTRAL_SERVER_URL}/get_file_metadata",
        json={"file_name": file_name},
    )

    file_metadata = json.loads(response.text)

    chunks = {}
    for chunk_id, replicas in file_metadata.items():
        chunk_downloaded = False

        for replica in replicas:
            data_node_id = replica["id"]

            try:
                data_node_url = requests.get(f"{CENTRAL_SERVER_URL}/get_data_node_url", params={"id": data_node_id}).text
                data_node_url += "/retrieve_file_chunk"
                print(f"Downloading chunk {chunk_id} from data node {data_node_id}")

                response = requests.post(
                    data_node_url,
                    json={
                        "file_name": file_name,
                        "chunk_id": int(chunk_id),
                    },
                )

                if response.status_code == 200:
                    chunk_data = json.loads(response.text)["file_data"]
                    chunks[int(chunk_id)] = b64decode(chunk_data.encode())
                    chunk_downloaded = True
                    break
            except requests.exceptions.RequestException as e:
                print(f"Failed to download chunk {chunk_id} from data node {data_node_id}: {e}")

        if not chunk_downloaded:
            print(f"Failed to download chunk {chunk_id}")
            return

    with open(output_path, "wb") as f:
        for chunk_id in sorted(chunks.keys()):
            f.write(chunks[chunk_id])

    print("File downloaded successfully")


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python client.py [upload|download] [file_path]")
        sys.exit(1)

    action, file_path = sys.argv[1], sys.argv[2]

    if action == "upload":
        upload_file(file_path)
    elif action == "download":
        output_file_path = os.path.join(os.path.dirname(file_path), f"downloaded_{os.path.basename(file_path)}")
        download_file(os.path.basename(file_path), output_file_path)
    else:
        print("Invalid action. Use 'upload' or 'download'.")

