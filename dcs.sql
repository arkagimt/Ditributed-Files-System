CREATE DATABASE dfs;
USE dfs;


CREATE TABLE IF NOT EXISTS data_nodes (
    id INT PRIMARY KEY AUTO_INCREMENT,
    url VARCHAR(255) NOT NULL
);

CREATE TABLE IF NOT EXISTS file_chunks (
    id INT PRIMARY KEY AUTO_INCREMENT,
    file_name VARCHAR(255) NOT NULL,
    chunk_id INT NOT NULL,
    data_node_id INT NOT NULL,
    replication_count INT NOT NULL,
    FOREIGN KEY (data_node_id) REFERENCES data_nodes(id)
);


INSERT INTO data_nodes VALUES (1, 'http://localhost:6001');
INSERT INTO data_nodes VALUES (2, 'http://localhost:6002');
INSERT INTO data_nodes VALUES (3, 'http://localhost:6003');



select * from file_chunks;

select * from data_nodes;



