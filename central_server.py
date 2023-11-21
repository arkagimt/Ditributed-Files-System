#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import json
import mysql.connector
from flask import Flask, request

app = Flask(__name__)

# Configure your MySQL connection details
config = {
    'user': 'root',
    'password': '@A9232695645g',
    'host': 'localhost',
    'database': "dfs"
}

def add_file_chunk(file_name, chunk_id, data_node_id, replication_count):
    conn = mysql.connector.connect(**config)
    cursor = conn.cursor()
    cursor.execute(
        """
        INSERT INTO file_chunks (file_name, chunk_id, data_node_id, replication_count)
        VALUES (%s, %s, %s, %s)
        """,
        (file_name, chunk_id, data_node_id, replication_count),
    )
    conn.commit()
    conn.close()

@app.route("/upload_file", methods=["POST"])
def upload_file():
    file_name = request.json["file_name"]
    file_chunks = request.json["file_chunks"]

    replication_count = 2

    for chunk_id, data_node_ids in file_chunks.items():
        for data_node_id in data_node_ids:
            add_file_chunk(file_name, chunk_id, data_node_id, replication_count)

    return json.dumps({"status": "success"})

@app.route("/get_file_metadata", methods=["POST"])
def get_file_metadata():
    file_name = request.json["file_name"]

    conn = mysql.connector.connect(**config)
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT chunk_id, data_node_id, replication_count
        FROM file_chunks
        WHERE file_name = %s
        """,
        (file_name,),
    )
    result = cursor.fetchall()
    conn.close()

    file_metadata = {}
    for chunk_id, data_node_id, replication_count in result:
        if chunk_id not in file_metadata:
            file_metadata[chunk_id] = []
        file_metadata[chunk_id].append({"id": data_node_id, "replicas": replication_count})

    return json.dumps(file_metadata)

@app.route("/get_data_node_url", methods=["GET"])
def get_data_node_url():
    data_node_id = int(request.args.get("id"))
    conn = mysql.connector.connect(**config)
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT url FROM data_nodes WHERE id = %s
        """,
        (data_node_id,)
    )
    result = cursor.fetchone()
    conn.close()

    if result:
        return result[0]
    else:
        return "Data node not found", 404

if __name__ == "__main__":
    app.run()

