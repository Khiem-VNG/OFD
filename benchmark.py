# benchmark.py
import time
import statistics
from pymongo import MongoClient
from cassandra.cluster import Cluster
from cassandra.concurrent import execute_concurrent_with_args
import redis
from bson import ObjectId

db   = MongoClient("mongodb://admin:admin123@127.0.0.1:27017/?authSource=admin")["food_delivery"]
cass = Cluster(["localhost"], port=9042).connect("food_delivery")
r    = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)


def measure(fn, repeat=50):
    times = []
    for _ in range(repeat):
        t0 = time.perf_counter()
        fn()
        times.append((time.perf_counter() - t0) * 1000)
    return {
        "min":    round(min(times), 2),
        "max":    round(max(times), 2),
        "avg":    round(statistics.mean(times), 2),
        "median": round(statistics.median(times), 2),
    }


# ============================================================
# THỰC NGHIỆM 1: Hiệu suất đọc — MongoDB vs Redis cache
# ============================================================
def bench1_mongodb_vs_redis():
    print("\n" + "="*60)
    print("THỰC NGHIỆM 1: Đọc menu nhà hàng — MongoDB vs Redis")
    print("="*60)

    rest = db.restaurants.find_one({"is_active": True})
    rid  = str(rest["_id"])

    # Đảm bảo có cache trong Redis
    import json
    items = list(db.menu_items.find(
        {"restaurant_id": rest["_id"], "is_available": True},
        {"name": 1, "category": 1, "price": 1, "_id": 0}
    ))
    r.set(f"menu:{rid}", json.dumps(items, default=str), ex=300)

    # Đo MongoDB (không cache)
    def read_mongo():
        list(db.menu_items.find(
            {"restaurant_id": rest["_id"], "is_available": True},
            {"name": 1, "category": 1, "price": 1}
        ))

    # Đo Redis (có cache)
    def read_redis():
        data = r.get(f"menu:{rid}")
        json.loads(data)

    mongo_stats = measure(read_mongo, repeat=100)
    redis_stats = measure(read_redis, repeat=100)

    print(f"\n  MongoDB  | avg: {mongo_stats['avg']:>7} ms | median: {mongo_stats['median']:>7} ms")
    print(f"  Redis    | avg: {redis_stats['avg']:>7} ms | median: {redis_stats['median']:>7} ms")
    speedup = round(mongo_stats['avg'] / redis_stats['avg'], 1)
    print(f"\n  => Redis nhanh hơn MongoDB {speedup}x khi đọc menu")
    print(f"  => Lý do chọn Redis làm cache layer: giảm tải MongoDB {speedup}x")


# ============================================================
# THỰC NGHIỆM 2: Hiệu suất ghi — MongoDB vs Cassandra (time-series)
# ============================================================
def bench2_write_mongodb_vs_cassandra():
    print("\n" + "="*60)
    print("THỰC NGHIỆM 2: Ghi bulk activity log — MongoDB vs Cassandra")
    print("="*60)

    import uuid
    from datetime import datetime
    from cassandra.concurrent import execute_concurrent_with_args

    insert_cass = cass.prepare("""
        INSERT INTO customer_activity
        (customer_id, timestamp, event_id, event_type, restaurant_id, menu_item_id, search_keyword)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """)

    BATCH_SIZES = [10, 50, 100, 500, 1000, 2000]

    print(f"\n  {'Batch size':>12} | {'MongoDB (ms)':>14} | {'Cassandra (ms)':>14} | {'Winner':>10}")
    print(f"  {'-'*58}")

    for n in BATCH_SIZES:

        def write_mongo_bulk():
            docs = [{
                "customer_id":   ObjectId(),
                "timestamp":     datetime.now(),
                "event_type":    "VIEW_ITEM",
                "restaurant_id": ObjectId(),
            } for _ in range(n)]
            db.customer_activity.insert_many(docs, ordered=False)

        def write_cass_bulk():
            params = [
                (uuid.uuid4(), datetime.now(), uuid.uuid4(),
                 "VIEW_ITEM", uuid.uuid4(), None, None)
                for _ in range(n)
            ]
            execute_concurrent_with_args(cass, insert_cass, params, concurrency=10)

        mongo_stats = measure(write_mongo_bulk, repeat=20)
        cass_stats  = measure(write_cass_bulk,  repeat=20)

        winner = "MongoDB" if mongo_stats['avg'] < cass_stats['avg'] else "Cassandra"
        print(f"  {n:>12} | {mongo_stats['avg']:>14} | {cass_stats['avg']:>14} | {winner:>10}")

    print(f"""
  Phân tích:
  - Batch nhỏ (10-50):   MongoDB thường nhanh hơn do ít overhead
  - Batch lớn (100-500): Cassandra tối ưu hơn do LSM tree write path
  - Thực tế production:  activity log ghi hàng triệu events/ngày
                         → Cassandra phù hợp hơn về scalability
  - MongoDB dùng cho:    business data cần ACID, query linh hoạt
  - Cassandra dùng cho:  append-only, time-series, volume cực lớn
    """)

# ============================================================
# THỰC NGHIỆM 3: Truy vấn phức tạp — aggregation pipeline
# ============================================================
def bench3_complex_query():
    print("\n" + "="*60)
    print("THỰC NGHIỆM 3: Truy vấn phức tạp — thống kê doanh thu")
    print("="*60)

    from datetime import datetime

    # Truy vấn có index
    def query_with_index():
        list(db.orders.find(
            {"current_status": "COMPLETED"},
            {"total_amount": 1, "restaurant_id": 1}
        ).limit(100))

    # Aggregation pipeline
    def query_aggregation():
        list(db.orders.aggregate([
            {"$match": {"current_status": "COMPLETED"}},
            {"$group": {
                "_id": "$restaurant_id",
                "total": {"$sum": "$total_amount"},
                "count": {"$sum": 1}
            }},
            {"$sort": {"total": -1}},
            {"$limit": 10}
        ]))

    # Dùng daily_stats thay vì aggregate thẳng
    def query_precomputed():
        list(db.restaurant_daily_stats.find(
            {},
            {"restaurant_id": 1, "gross_revenue": 1}
        ).sort("gross_revenue", -1).limit(10))

    simple_stats = measure(query_with_index,  repeat=50)
    aggr_stats   = measure(query_aggregation, repeat=50)
    pre_stats    = measure(query_precomputed, repeat=50)

    print(f"\n  Simple query (indexed) | avg: {simple_stats['avg']:>7} ms")
    print(f"  Aggregation pipeline   | avg: {aggr_stats['avg']:>7} ms")
    print(f"  Pre-computed stats     | avg: {pre_stats['avg']:>7} ms")
    speedup = round(aggr_stats['avg'] / pre_stats['avg'], 1)
    print(f"\n  => Pre-computed (restaurant_daily_stats) nhanh hơn {speedup}x")
    print(f"  => Lý do dùng daily_stats collection: tránh aggregate realtime")


# ============================================================
# THỰC NGHIỆM 4: Khả năng mở rộng schema — MongoDB flexible
# ============================================================
def bench4_schema_flexibility():
    print("\n" + "="*60)
    print("THỰC NGHIỆM 4: Khả năng mở rộng — thêm thuộc tính mới")
    print("="*60)

    # MongoDB: thêm field mới không cần ALTER TABLE
    t0 = time.perf_counter()
    result = db.menu_items.update_many(
        {"category": "main"},
        {"$set": {"nutritional_info": {"calories": 450, "protein": 25}}}
    )
    t1 = time.perf_counter()
    mongo_time = round((t1 - t0) * 1000, 2)

    print(f"\n  MongoDB: thêm field 'nutritional_info' vào {result.modified_count} documents")
    print(f"  Thời gian: {mongo_time} ms — không cần ALTER TABLE, không downtime")
    print(f"\n  => Lý do chọn MongoDB: schema linh hoạt, mở rộng không ảnh hưởng hệ thống")

    # Rollback
    db.menu_items.update_many({}, {"$unset": {"nutritional_info": ""}})


# ============================================================
# MAIN
# ============================================================
if __name__ == "__main__":
    print("🚀 BẮT ĐẦU BENCHMARK\n")
    bench1_mongodb_vs_redis()
    bench2_write_mongodb_vs_cassandra()
    bench3_complex_query()
    bench4_schema_flexibility()
    print("\n✅ Benchmark hoàn tất!")