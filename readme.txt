1. Run these scripts step by steps.
2. To check fault tolerance we can 1st stop any data node and then run the download comand


python central_server.py

python data_node1.py

python data_node2.py

python data_node3.py

python client.py upload "C:\Users\arkag\OneDrive\Desktop\SDE\sample.txt"

python client.py download "C:\Users\arkag\OneDrive\Desktop\SDE\sample.txt"

cd C:\Users\arkag\OneDrive\Desktop\SDE