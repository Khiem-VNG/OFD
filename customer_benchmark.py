# customer_benchmark.py
import time
import statistics
import json
import uuid
import random
from datetime import datetime, timedelta
from pymongo import MongoClient
from cassandra.cluster import Cluster
import redis
from bson import ObjectId

# ============================================================
# KẾT NỐI
# ============================================================
db   = MongoClient("mongodb://admin:admin123@127.0.0.1:27017/?authSource=admin")["food_delivery"]
cass = Cluster(["localhost"], port=9042).connect("food_delivery")
r    = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)

# ============================================================
# HELPER
# ============================================================
def measure(fn, repeat=50):
    times = []
    for _ in range(repeat):
        t0 = time.perf_counter()
        fn()
        times.append((time.perf_counter() - t0) * 1000)
    return {
        "min":    round(min(times), 3),
        "max":    round(max(times), 3),
        "avg":    round(statistics.mean(times), 3),
        "median": round(statistics.median(times), 3),
        "p95":    round(sorted(times)[int(len(times) * 0.95)], 3),
    }

def print_result(label, stats):
    print(f"  {label:<35} avg={stats['avg']:>8} ms | "
          f"median={stats['median']:>8} ms | "
          f"p95={stats['p95']:>8} ms")

def divider(title=""):
    if title:
        print(f"\n{'='*65}")
        print(f"  {title}")
        print(f"{'='*65}")
    else:
        print(f"  {'-'*63}")

# ============================================================
# LẤY DỮ LIỆU MẪU
# ============================================================
sample_restaurant = db.restaurants.find_one({"is_active": True})
sample_customer   = db.customers.find_one({"status": "active"})
sample_order      = db.orders.find_one({"current_status": "COMPLETED"})
rid               = sample_restaurant["_id"]
rid_str           = str(rid)
cid_mongo         = sample_customer["_id"]
cid_cass          = uuid.uuid5(uuid.NAMESPACE_OID, str(cid_mongo))

# ============================================================
# BENCHMARK 1: ĐỌC MENU NHÀ HÀNG
# Scenario: Customer mở trang nhà hàng → load menu
# So sánh: MongoDB trực tiếp vs Redis cache vs MongoDB+Redis
# ============================================================
def bench1_read_menu():
    divider("BENCHMARK 1: Đọc menu nhà hàng")
    print("  Scenario: Customer mở trang nhà hàng → load danh sách món\n")

    # Chuẩn bị Redis cache
    items = list(db.menu_items.find(
        {"restaurant_id": rid, "is_available": True},
        {"name": 1, "price": 1, "category": 1, "_id": 0}
    ))
    r.set(f"menu:{rid_str}", json.dumps(items, default=str), ex=300)

    # Phương án A: MongoDB mỗi lần đọc (không cache)
    def read_mongodb_only():
        list(db.menu_items.find(
            {"restaurant_id": rid, "is_available": True},
            {"name": 1, "price": 1, "category": 1}
        ))

    # Phương án B: Redis cache (thiết kế hiện tại)
    def read_redis_cache():
        data = r.get(f"menu:{rid_str}")
        json.loads(data)

    # Phương án C: MongoDB không có index (giả lập xấu)
    def read_mongodb_no_index():
        list(db.menu_items.find(
            {"restaurant_id": rid},
            {"name": 1, "price": 1, "category": 1}
        ).limit(50))

    # Phương án D: Cassandra (không phù hợp — demo để so sánh)
    def read_cassandra_workaround():
        # Cassandra không có index linh hoạt như MongoDB
        # phải query theo partition key customer_id, không query theo restaurant
        # → phải dùng ALLOW FILTERING (rất chậm trong production)
        list(cass.execute(
            "SELECT event_type FROM customer_activity WHERE customer_id = %s LIMIT 10",
            (cid_cass,)
        ))

    results = {
        "MongoDB trực tiếp":      measure(read_mongodb_only,        repeat=100),
        "Redis cache (hiện tại)": measure(read_redis_cache,         repeat=100),
        "MongoDB không index":    measure(read_mongodb_no_index,    repeat=100),
        "Cassandra (workaround)": measure(read_cassandra_workaround, repeat=100),
    }

    for label, stats in results.items():
        print_result(label, stats)


# ============================================================
# BENCHMARK 2: GHI ACTIVITY LOG
# Scenario: Customer xem món, thêm giỏ hàng → ghi activity
# So sánh: MongoDB vs Cassandra (thiết kế hiện tại)
# ============================================================
def bench2_write_activity():
    divider("BENCHMARK 2: Ghi activity log")
    print("  Scenario: Customer xem/thêm món → ghi event log\n")

    from cassandra.concurrent import execute_concurrent_with_args

    insert_cass = cass.prepare("""
        INSERT INTO customer_activity
        (customer_id, timestamp, event_id, event_type,
         restaurant_id, menu_item_id, search_keyword)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """)

    SIZES = [1, 10, 50, 100]

    print(f"  {'Batch':>6} | {'MongoDB (ms)':>14} | {'Cassandra (ms)':>14} | {'Winner':>12}")
    print(f"  {'-'*55}")

    for n in SIZES:
        def write_mongo(n=n):
            docs = [{
                "customer_id":   cid_mongo,
                "timestamp":     datetime.now(),
                "event_type":    "VIEW_ITEM",
                "restaurant_id": rid,
            } for _ in range(n)]
            db.customer_activity.insert_many(docs, ordered=False)

        def write_cassandra(n=n):
            params = [
                (cid_cass, datetime.now(), uuid.uuid4(),
                 "VIEW_ITEM",
                 uuid.uuid5(uuid.NAMESPACE_OID, rid_str),
                 None, None)
                for _ in range(n)
            ]
            execute_concurrent_with_args(
                cass, insert_cass, params, concurrency=min(n, 20)
            )

        m_stats = measure(write_mongo,     repeat=30)
        c_stats = measure(write_cassandra, repeat=30)
        winner  = "MongoDB" if m_stats["avg"] < c_stats["avg"] else "Cassandra ✓"
        print(f"  {n:>6} | {m_stats['avg']:>14} | {c_stats['avg']:>14} | {winner:>12}")


# ============================================================
# BENCHMARK 3: LẤY LỊCH SỬ ĐƠN HÀNG
# Scenario: Customer vào "Đơn hàng của tôi" → load danh sách
# So sánh: MongoDB trực tiếp vs Cassandra (thiết kế hiện tại)
# ============================================================
def bench3_order_history():
    divider("BENCHMARK 3: Lấy lịch sử đơn hàng")
    print("  Scenario: Customer vào xem danh sách đơn hàng\n")

    order_count = db.orders.count_documents({"customer_id": cid_mongo})
    print(f"  Số đơn của customer này: {order_count}\n")

    # MongoDB có index
    def read_mongo_indexed():
        list(db.orders.find(
            {"customer_id": cid_mongo},
            {"current_status": 1, "total_amount": 1,
             "created_at": 1, "restaurant_id": 1}
        ).sort("created_at", -1).limit(20))

    # Cassandra partition by customer_id
    def read_cass_history():
        list(cass.execute("""
            SELECT order_id, created_at, restaurant_name,
                   current_status, total_amount, item_summary
            FROM order_history_by_customer
            WHERE customer_id = %s
            LIMIT 20
        """, (cid_cass,)))

    results = {
        "MongoDB có index":     measure(read_mongo_indexed, repeat=100),
        "Cassandra (hiện tại)": measure(read_cass_history,  repeat=100),
    }

    for label, stats in results.items():
        print_result(label, stats)


# ============================================================
# BENCHMARK 4: TRẠNG THÁI ĐƠN REAL-TIME
# Scenario: Customer theo dõi trạng thái đơn đang giao
# So sánh: MongoDB vs Redis (thiết kế hiện tại) vs Cassandra
# ============================================================
def bench4_order_status_realtime():
    divider("BENCHMARK 4: Theo dõi trạng thái đơn real-time")
    print("  Scenario: Customer refresh xem đơn đang ở bước nào\n")

    oid_str = str(sample_order["_id"]) if sample_order else str(ObjectId())
    r.hset(f"order:status:{oid_str}", mapping={
        "status":      "DELIVERING",
        "updated_at":  datetime.now().isoformat(),
        "customer_id": str(cid_mongo),
    })
    r.expire(f"order:status:{oid_str}", 7200)

    # MongoDB: query document đầy đủ
    def read_mongo_status():
        if sample_order:
            db.orders.find_one(
                {"_id": sample_order["_id"]},
                {"current_status": 1, "updated_at": 1}
            )

    # Redis: lấy hash field (thiết kế hiện tại)
    def read_redis_status():
        r.hgetall(f"order:status:{oid_str}")

    # Cassandra: không phù hợp cho point lookup
    def read_cass_status():
        # Cassandra cần biết partition key (customer_id)
        # không query trực tiếp theo order_id
        list(cass.execute("""
            SELECT current_status, created_at
            FROM order_history_by_customer
            WHERE customer_id = %s
            LIMIT 1
        """, (cid_cass,)))

    results = {
        "MongoDB":                measure(read_mongo_status, repeat=200),
        "Redis (hiện tại)":       measure(read_redis_status, repeat=200),
        "Cassandra (workaround)": measure(read_cass_status,  repeat=200),
    }

    for label, stats in results.items():
        print_result(label, stats)


# ============================================================
# BENCHMARK 5: TẠO ĐƠN HÀNG
# Scenario: Customer xác nhận đặt đơn → lưu vào DB
# So sánh: chỉ MongoDB vs MongoDB + Redis + Cassandra (hiện tại)
# ============================================================
def bench5_create_order():
    divider("BENCHMARK 5: Tạo đơn hàng")
    print("  Scenario: Customer bấm đặt hàng → lưu đơn\n")

    from cassandra.concurrent import execute_concurrent_with_args

    insert_cass = cass.prepare("""
        INSERT INTO order_history_by_customer
        (customer_id, created_at, order_id, restaurant_name,
         current_status, total_amount, payment_method, item_summary)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """)

    def make_order_doc():
        now = datetime.now()
        return {
            "_id":               ObjectId(),
            "customer_id":       cid_mongo,
            "restaurant_id":     rid,
            "delivery_street":   "123 Test St",
            "delivery_district": "Quan 1",
            "delivery_city":     "Ho Chi Minh",
            "items": [{
                "menu_item_id": ObjectId(),
                "name":         "Bun Bo Hue",
                "category":     "main",
                "quantity":     2,
                "unit_price":   65000,
                "line_total":   130000,
            }],
            "status_history":  [{"status": "PLACED", "timestamp": now}],
            "current_status":  "PLACED",
            "payment_method":  "MOMO",
            "items_subtotal":  130000,
            "delivery_fee":    15000,
            "discount_amount": 0,
            "total_amount":    145000,
            "cancel_reason":   None,
            "created_at":      now,
            "updated_at":      now,
        }

    # Phương án A: chỉ MongoDB
    def create_mongo_only():
        order = make_order_doc()
        db.orders.insert_one(order)
        # Cleanup
        db.orders.delete_one({"_id": order["_id"]})

    # Phương án B: MongoDB + Redis + Cassandra (thiết kế hiện tại)
    def create_full_pipeline():
        order = make_order_doc()
        oid   = str(order["_id"])
        now   = order["created_at"]

        # 1. MongoDB (source of truth)
        db.orders.insert_one(order)

        # 2. Redis cache status
        r.hset(f"order:status:{oid}", mapping={
            "status":      "PLACED",
            "updated_at":  now.isoformat(),
            "customer_id": str(cid_mongo),
        })
        r.expire(f"order:status:{oid}", 7200)

        # 3. Cassandra order_history
        cass.execute(insert_cass, (
            cid_cass, now,
            uuid.uuid5(uuid.NAMESPACE_OID, oid),
            sample_restaurant["name"],
            "PLACED", 145000, "MOMO", "Bun Bo Hue x2"
        ))

        # Cleanup
        db.orders.delete_one({"_id": order["_id"]})
        r.delete(f"order:status:{oid}")

    results = {
        "Chỉ MongoDB":                        measure(create_mongo_only,    repeat=50),
        "MongoDB + Redis + Cassandra (hiện tại)": measure(create_full_pipeline, repeat=50),
    }

    for label, stats in results.items():
        print_result(label, stats)


# ============================================================
# BENCHMARK 6: MỞ RỘNG SCHEMA
# Scenario: Thêm thuộc tính mới vào menu_items
# So sánh: MongoDB update_many vs SQL-style row by row
# ============================================================
def bench6_schema_flexibility():
    divider("BENCHMARK 6: Khả năng mở rộng schema")
    print("  Scenario: Thêm field 'nutritional_info' vào tất cả món ăn\n")

    count = db.menu_items.count_documents({})

    # MongoDB: thêm field mới không cần ALTER TABLE
    def mongo_add_field():
        db.menu_items.update_many(
            {"category": "main"},
            {"$set": {"nutritional_info": {"calories": 450, "protein": 25}}}
        )
        # Rollback
        db.menu_items.update_many(
            {},
            {"$unset": {"nutritional_info": ""}}
        )

    # Mô phỏng SQL: phải update từng row (không có ALTER ADD COLUMN tức thì)
    def simulate_sql_migration():
        items = list(db.menu_items.find({"category": "main"}, {"_id": 1}))
        for item in items:
            db.menu_items.update_one(
                {"_id": item["_id"]},
                {"$set": {"nutritional_info": {"calories": 450, "protein": 25}}}
            )
        # Rollback
        db.menu_items.update_many(
            {},
            {"$unset": {"nutritional_info": ""}}
        )

    results = {
        f"MongoDB update_many ({count} docs)":  measure(mongo_add_field,       repeat=20),
        f"SQL-style row by row ({count} rows)": measure(simulate_sql_migration, repeat=20),
    }

    for label, stats in results.items():
        print_result(label, stats)


# ============================================================
# MAIN
# ============================================================
if __name__ == "__main__":
    print("\n🚀 CUSTOMER FLOW BENCHMARK")
    print("   So sánh hiệu suất kiến trúc MongoDB + Redis + Cassandra")
    print("   với các phương án lưu trữ thay thế\n")

    bench1_read_menu()
    bench2_write_activity()
    bench3_order_history()
    bench4_order_status_realtime()
    bench5_create_order()
    bench6_schema_flexibility()

    print("\n✅ Benchmark hoàn tất!\n")