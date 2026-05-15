# OFD
Online foode delivery

# Các bước thực hiện
        # Bước 1: Khởi động tất cả docker
docker-compose up -d

        # Bước 2: Kiểm tra status
docker-compose ps

# Cassandra cần ~60s để khởi động hoàn toàn
# Kiểm tra Cassandra ready chưa
docker exec food_cassandra cqlsh -e "describe keyspaces"

        # Bước 3: Setup Python environment

# Nên sử dụng python version 3.11 trở xuống (bản python 3.13 cassandra cluster chưa hỗ trợ)
python -m venv venv
venv\Scripts\activate
pip install pymongo cassandra-driver redis faker neo4j pyasyncore

        # Bước 4: Chạy các file trong đồ án 

# Tạo schema cho database
python Schema.py
pyhton user_activity_graph.py

# Tạo data với lượng nhỏ
python gen_data.py
# Tạo data với lượng lớn
python new_gen.py

# Chạy các file
python customer.py
python restaurant.py
python recommendation.py

# Test benchmark
python customer_benchmark.py
python restaurant_benchmark.py
graph_benchmark.py 





