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
pip install pymongo cassandra-driver redis faker

        # Bước 4: Chạy các file trong đồ án

# Tạo schema cho các database
python schema.py
# Tạo dữ liệu giả để chạy test + đo benchmark
python gen_data.py
# Chạy các chức năng đã cài đặt
python demo_queries.py
# Chạy benchmark để đo tốc độ đọc/ghi các database đã chọn so với loại nosql khác
python benchmark.py