import time
import statistics
import json
import uuid
from datetime import datetime, timedelta, timezone
from pymongo import MongoClient
from cassandra.cluster import Cluster
from cassandra.concurrent import execute_concurrent_with_args
import redis
from bson import ObjectId

# ============================================================
# KẾT NỐI — giữ nguyên như customer_benchmark.py
# ============================================================
db   = MongoClient("mongodb://admin:admin123@127.0.0.1:27017/?authSource=admin")["food_delivery"]
cass = Cluster(["localhost"], port=9042).connect("food_delivery")
r    = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)

VALID_STATUSES = ["PLACED", "CONFIRMED", "PREPARING", "PICKED_UP",
                  "DELIVERING", "COMPLETED", "CANCELLED"]

# ============================================================
# HELPER — giữ nguyên như customer_benchmark.py
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
    print(f"  {label:<45} avg={stats['avg']:>8} ms | "
          f"median={stats['median']:>8} ms | "
          f"p95={stats['p95']:>8} ms")

def print_winner(results: dict):
    best_label = min(results, key=lambda k: results[k]["avg"])
    print(f"\n  🏆 Nhanh nhất: {best_label} "
          f"({results[best_label]['avg']} ms avg)")
    for label, stats in results.items():
        if label != best_label:
            ratio = round(stats["avg"] / results[best_label]["avg"], 1)
            print(f"     {label} chậm hơn {ratio}x")

def divider(title=""):
    if title:
        print(f"\n{'='*70}")
        print(f"  {title}")
        print(f"{'='*70}")
    else:
        print(f"  {'-'*68}")

# ============================================================
# LẤY DỮ LIỆU MẪU
# ============================================================
sample_restaurant = db.restaurants.find_one({"is_active": True})
sample_order      = db.orders.find_one({"current_status": "COMPLETED"})
sample_customer   = db.customers.find_one({"status": "active"})
rid               = sample_restaurant["_id"]
rid_str           = str(rid)
rid_cass          = uuid.uuid5(uuid.NAMESPACE_OID, rid_str)
cid_mongo         = sample_customer["_id"]
cid_cass          = uuid.uuid5(uuid.NAMESPACE_OID, str(cid_mongo))

# Chuẩn bị Cassandra prepared statements
stmt_insert_activity = cass.prepare("""
    INSERT INTO customer_activity
    (customer_id, timestamp, event_id, event_type,
     restaurant_id, menu_item_id, search_keyword)
    VALUES (?, ?, ?, ?, ?, ?, ?)
""")

stmt_read_history = cass.prepare("""
    SELECT order_id, created_at, restaurant_name,
           current_status, total_amount, item_summary
    FROM order_history_by_customer
    WHERE customer_id = ?
    LIMIT 20
""")

stmt_insert_history = cass.prepare("""
    INSERT INTO order_history_by_customer
    (customer_id, created_at, order_id, restaurant_name,
     current_status, total_amount, payment_method, item_summary)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
""")


# ============================================================
# BENCHMARK 1: DANH SÁCH NHÀ HÀNG
# Chức năng: list_and_select_restaurant()
# So sánh: MongoDB vs Redis vs Cassandra (workaround)
# ============================================================
def bench1_list_restaurants():
    divider("BENCHMARK 1: Danh sách nhà hàng")
    print("  Chức năng: list_and_select_restaurant()\n")

    # Chuẩn bị Redis
    all_restaurants = list(db.restaurants.find())
    r.set("restaurants:list", json.dumps(all_restaurants, default=str), ex=120)

    def read_mongodb():
        list(db.restaurants.find())

    def read_redis():
        data = r.get("restaurants:list")
        json.loads(data)

    def read_cassandra():
        # Cassandra không có table restaurants
        # Workaround: query customer_activity để lấy danh sách restaurant_id distinct
        # Đây là cách gần nhất Cassandra có thể làm — không phù hợp cho use case này
        list(cass.execute(
            "SELECT restaurant_id FROM customer_activity WHERE customer_id = %s LIMIT 50",
            (cid_cass,)
        ))

    results = {
        "MongoDB":                    measure(read_mongodb,   repeat=100),
        "Redis":                      measure(read_redis,     repeat=100),
        "Cassandra (workaround)":     measure(read_cassandra, repeat=100),
    }

    for label, stats in results.items():
        print_result(label, stats)
    print_winner(results)
    print(f"""
  Phân tích:
  → Redis nhanh nhất vì data nằm trong RAM, không cần I/O
  → Cassandra không có table restaurants → phải workaround
    qua customer_activity, kết quả không đầy đủ và không chính xác
  → Cassandra được thiết kế cho write-heavy time-series,
    không phải cho query danh sách entity như nhà hàng
  → Kết luận: MongoDB + Redis cache là lựa chọn đúng đắn
    """)


# ============================================================
# BENCHMARK 2: XEM MENU NHÀ HÀNG
# Chức năng: view_menu(restaurant)
# So sánh: MongoDB vs Redis vs Cassandra (workaround)
# ============================================================
def bench2_view_menu():
    divider("BENCHMARK 2: Xem menu nhà hàng")
    print("  Chức năng: view_menu(restaurant)\n")

    # Chuẩn bị Redis
    items = list(db.menu_items.find({"restaurant_id": rid}).sort("category", 1))
    r.set(f"menu:full:{rid_str}", json.dumps(items, default=str), ex=180)

    def read_mongodb():
        list(db.menu_items.find({"restaurant_id": rid}).sort("category", 1))

    def read_redis():
        data = r.get(f"menu:full:{rid_str}")
        json.loads(data)

    def read_cassandra():
        # Cassandra không có table menu_items
        # Workaround: query customer_activity lấy menu_item_id đã xem
        # → chỉ lấy được menu item mà customer từng interact, không phải full menu
        list(cass.execute(
            """SELECT menu_item_id, event_type FROM customer_activity
               WHERE customer_id = %s LIMIT 50""",
            (cid_cass,)
        ))

    results = {
        "MongoDB":                measure(read_mongodb,   repeat=100),
        "Redis":                  measure(read_redis,     repeat=100),
        "Cassandra (workaround)": measure(read_cassandra, repeat=100),
    }

    for label, stats in results.items():
        print_result(label, stats)
    print_winner(results)
    count = db.menu_items.count_documents({"restaurant_id": rid})
    print(f"""
  Phân tích ({count} món trong menu):
  → Redis nhanh nhất, menu ít thay đổi → TTL 180s phù hợp
  → Cassandra workaround chỉ lấy được món đã có activity,
    không thể trả về full menu → không dùng được cho chức năng này
  → Cassandra thiếu index theo restaurant_id cho menu_items
    → ALLOW FILTERING rất chậm trong production
  → Kết luận: MongoDB + Redis là lựa chọn duy nhất phù hợp
    """)


# ============================================================
# BENCHMARK 3: THÊM MÓN ĂN MỚI
# Chức năng: add_dish(restaurant)
# So sánh: MongoDB vs MongoDB+Redis vs MongoDB+Redis+Cassandra
# ============================================================
def bench3_add_dish():
    divider("BENCHMARK 3: Thêm món ăn mới")
    print("  Chức năng: add_dish(restaurant)\n")

    inserted_ids = []

    def make_dish():
        return {
            "restaurant_id": rid,
            "name":          "Món test benchmark",
            "category":      "main",
            "price":         50000,
            "is_available":  True,
            "description":   "Test",
            "updated_at":    datetime.now(timezone.utc),
        }

    def write_mongodb():
        result = db.menu_items.insert_one(make_dish())
        inserted_ids.append(result.inserted_id)

    def write_mongodb_redis():
        result = db.menu_items.insert_one(make_dish())
        inserted_ids.append(result.inserted_id)
        r.delete(f"menu:full:{rid_str}")
        r.delete(f"menu:{rid_str}")

    def write_mongodb_redis_cassandra():
        # Ghi MongoDB + invalidate Redis + log activity vào Cassandra
        result = db.menu_items.insert_one(make_dish())
        inserted_ids.append(result.inserted_id)
        r.delete(f"menu:full:{rid_str}")
        r.delete(f"menu:{rid_str}")
        # Ghi event "nhà hàng thêm món" vào Cassandra activity log
        cass.execute(stmt_insert_activity, (
            cid_cass,
            datetime.now(timezone.utc),
            uuid.uuid4(),
            "MENU_ITEM_ADDED",
            rid_cass,
            uuid.uuid5(uuid.NAMESPACE_OID, str(result.inserted_id)),
            None
        ))

    results = {
        "MongoDB":                        measure(write_mongodb,                 repeat=50),
        "MongoDB + Redis delete":         measure(write_mongodb_redis,           repeat=50),
        "MongoDB + Redis + Cassandra log": measure(write_mongodb_redis_cassandra, repeat=50),
    }

    # Cleanup
    db.menu_items.delete_many({"_id": {"$in": inserted_ids}})

    for label, stats in results.items():
        print_result(label, stats)

    overhead = round(
        results["MongoDB + Redis + Cassandra log"]["avg"] /
        results["MongoDB"]["avg"], 1
    )
    print(f"""
  Phân tích:
  → Pipeline đầy đủ chậm hơn ~{overhead}x so với chỉ MongoDB
  → Cassandra log hữu ích để track lịch sử thay đổi menu
    (khi nào thêm, ai thêm, thêm món gì)
  → Redis delete đảm bảo customer không thấy menu cũ
  → Ghi là thao tác 1 lần → overhead chấp nhận được
    """)


# ============================================================
# BENCHMARK 4: XEM TẤT CẢ ĐƠN HÀNG
# Chức năng: view_all_orders(restaurant)
# So sánh: MongoDB vs Redis vs Cassandra
# ============================================================
def bench4_view_all_orders():
    divider("BENCHMARK 4: Xem tất cả đơn hàng")
    print("  Chức năng: view_all_orders(restaurant)\n")

    # Chuẩn bị Redis
    orders = list(db.orders.find({"restaurant_id": rid}))
    r.set(f"orders:all:{rid_str}", json.dumps(orders, default=str), ex=30)

    def read_mongodb():
        list(db.orders.find({"restaurant_id": rid}))

    def read_redis():
        data = r.get(f"orders:all:{rid_str}")
        json.loads(data)

    def read_cassandra():
        # order_history_by_customer partition theo customer_id
        # → không thể query theo restaurant_id trực tiếp
        # Workaround: lấy lịch sử đơn của 1 customer, lọc theo restaurant
        rows = list(cass.execute(stmt_read_history, (cid_cass,)))
        # Lọc theo restaurant trên Python — không scalable
        [r for r in rows if r.restaurant_name == sample_restaurant.get("name")]

    results = {
        "MongoDB":                measure(read_mongodb,   repeat=100),
        "Redis":                  measure(read_redis,     repeat=100),
        "Cassandra (workaround)": measure(read_cassandra, repeat=100),
    }

    for label, stats in results.items():
        print_result(label, stats)
    print_winner(results)
    count = db.orders.count_documents({"restaurant_id": rid})
    print(f"""
  Phân tích ({count} đơn):
  → Cassandra partition theo customer_id, không theo restaurant_id
    → phải scan nhiều partition rồi filter trên Python
    → cực kỳ không hiệu quả khi số customer lớn
  → Redis nhanh nhưng TTL 30s vì đơn thay đổi thường xuyên
  → MongoDB là lựa chọn phù hợp nhất cho chức năng này
    """)


# ============================================================
# BENCHMARK 5: LỌC ĐƠN THEO TRẠNG THÁI
# Chức năng: view_orders_by_status(restaurant)
# So sánh: MongoDB vs Redis vs Cassandra
# ============================================================
def bench5_filter_by_status():
    divider("BENCHMARK 5: Lọc đơn theo trạng thái")
    print("  Chức năng: view_orders_by_status(restaurant)\n")

    target_status = "CONFIRMED"

    # Chuẩn bị Redis
    filtered = list(db.orders.find({
        "restaurant_id": rid,
        "current_status": target_status
    }))
    r.set(f"orders:{rid_str}:{target_status}",
          json.dumps(filtered, default=str), ex=30)

    def read_mongodb():
        list(db.orders.find({
            "restaurant_id": rid,
            "current_status": target_status
        }))

    def read_redis():
        data = r.get(f"orders:{rid_str}:{target_status}")
        if data:
            json.loads(data)

    def read_cassandra():
        # Cassandra order_history không có secondary index trên current_status
        # Workaround: lấy tất cả đơn của customer rồi filter status trên Python
        rows = list(cass.execute(stmt_read_history, (cid_cass,)))
        [row for row in rows if row.current_status == target_status]

    results = {
        "MongoDB":                measure(read_mongodb,   repeat=100),
        "Redis":                  measure(read_redis,     repeat=100),
        "Cassandra (workaround)": measure(read_cassandra, repeat=100),
    }

    for label, stats in results.items():
        print_result(label, stats)
    print_winner(results)
    count = db.orders.count_documents({
        "restaurant_id": rid, "current_status": target_status
    })
    print(f"""
  Phân tích ({count} đơn '{target_status}'):
  → Cassandra không hỗ trợ filter theo current_status mà không có
    partition key → phải lấy hết rồi filter trên Python
  → Redis TTL 30s vì status thay đổi liên tục → cache miss nhiều
  → MongoDB với compound index (restaurant_id, current_status)
    là lựa chọn đúng đắn nhất cho chức năng này
    """)


# ============================================================
# BENCHMARK 6: CẬP NHẬT TRẠNG THÁI ĐƠN HÀNG
# Chức năng: update_order_status(restaurant)
# So sánh: MongoDB vs MongoDB+Redis vs MongoDB+Redis+Cassandra
# ============================================================
def bench6_update_order_status():
    divider("BENCHMARK 6: Cập nhật trạng thái đơn hàng")
    print("  Chức năng: update_order_status(restaurant)\n")

    test_order = db.orders.find_one({"restaurant_id": rid})
    if not test_order:
        print("  ⚠ Không có đơn hàng để test.")
        return

    oid     = test_order["_id"]
    oid_str = str(oid)
    orig    = test_order["current_status"]
    oid_cass = uuid.uuid5(uuid.NAMESPACE_OID, oid_str)

    r.hset(f"order:status:{oid_str}", mapping={
        "status":     orig,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    })
    r.expire(f"order:status:{oid_str}", 7200)

    statuses = ["CONFIRMED", "PLACED", "CONFIRMED", "PLACED",
                "PREPARING", "PLACED", "CONFIRMED", "PLACED"]
    idx = [0]

    def write_mongodb():
        new_stat = statuses[idx[0] % len(statuses)]
        idx[0] += 1
        db.orders.update_one(
            {"_id": oid},
            {"$set": {"current_status": new_stat}}
        )

    def write_mongodb_redis():
        new_stat = statuses[idx[0] % len(statuses)]
        idx[0] += 1
        now = datetime.now(timezone.utc)
        db.orders.update_one(
            {"_id": oid},
            {"$set": {"current_status": new_stat, "updated_at": now}}
        )
        r.hset(f"order:status:{oid_str}", mapping={
            "status":     new_stat,
            "updated_at": now.isoformat(),
        })

    def write_mongodb_redis_cassandra():
        # MongoDB update + Redis sync + Cassandra UPDATE order_history
        new_stat = statuses[idx[0] % len(statuses)]
        idx[0] += 1
        now = datetime.now(timezone.utc)
        db.orders.update_one(
            {"_id": oid},
            {"$set": {"current_status": new_stat, "updated_at": now}}
        )
        r.hset(f"order:status:{oid_str}", mapping={
            "status":     new_stat,
            "updated_at": now.isoformat(),
        })
        # Cassandra: ghi lại event status change vào activity log
        cass.execute(stmt_insert_activity, (
            cid_cass,
            now,
            uuid.uuid4(),
            f"ORDER_{new_stat}",
            rid_cass,
            None,
            None
        ))

    results = {
        "MongoDB":                         measure(write_mongodb,                  repeat=100),
        "MongoDB + Redis sync":             measure(write_mongodb_redis,            repeat=100),
        "MongoDB + Redis + Cassandra log":  measure(write_mongodb_redis_cassandra,  repeat=100),
    }

    # Restore
    db.orders.update_one({"_id": oid}, {"$set": {"current_status": orig}})

    for label, stats in results.items():
        print_result(label, stats)

    overhead = round(
        results["MongoDB + Redis + Cassandra log"]["avg"] /
        results["MongoDB"]["avg"], 1
    )
    print(f"""
  Phân tích:
  → Pipeline đầy đủ chậm hơn ~{overhead}x so với chỉ MongoDB
  → Cassandra log mỗi lần đổi status → audit trail đầy đủ
    (biết chính xác lúc nào PLACED → CONFIRMED → PREPARING...)
  → Redis sync → customer thấy status mới ngay lập tức
  → Nhà hàng update status ít lần → overhead chấp nhận được
    """)


# ============================================================
# BENCHMARK 7: THỐNG KÊ TỔNG DOANH THU
# Chức năng: show_total_income(restaurant)
# So sánh: MongoDB aggregate vs Redis vs Cassandra
# ============================================================
def bench7_total_income():
    divider("BENCHMARK 7: Thống kê tổng doanh thu")
    print("  Chức năng: show_total_income(restaurant)\n")

    # Chuẩn bị Redis
    agg = list(db.orders.aggregate([
        {"$match": {"restaurant_id": rid, "current_status": "COMPLETED"}},
        {"$group": {"_id": None, "total": {"$sum": "$total_amount"}, "count": {"$sum": 1}}}
    ]))
    cached = {"total": agg[0]["total"], "count": agg[0]["count"]} if agg else {"total": 0, "count": 0}
    r.set(f"stats:income:{rid_str}", json.dumps(cached), ex=300)

    def read_mongodb():
        list(db.orders.aggregate([
            {"$match": {"restaurant_id": rid, "current_status": "COMPLETED"}},
            {"$group": {"_id": None, "total": {"$sum": "$total_amount"}, "count": {"$sum": 1}}}
        ]))

    def read_redis():
        data = r.get(f"stats:income:{rid_str}")
        json.loads(data)

    def read_cassandra():
        # Cassandra không hỗ trợ SUM aggregate trực tiếp theo restaurant_id
        # Workaround: lấy tất cả đơn của customer rồi tính tổng trên Python
        rows = list(cass.execute(stmt_read_history, (cid_cass,)))
        total = sum(
            row.total_amount for row in rows
            if row.current_status == "COMPLETED"
            and row.restaurant_name == sample_restaurant.get("name")
        )

    results = {
        "MongoDB aggregate":      measure(read_mongodb,   repeat=100),
        "Redis cache":            measure(read_redis,     repeat=100),
        "Cassandra (workaround)": measure(read_cassandra, repeat=100),
    }

    for label, stats in results.items():
        print_result(label, stats)
    print_winner(results)
    print(f"""
  Phân tích:
  → Cassandra không có aggregation SUM theo restaurant_id
    → phải kéo toàn bộ data về Python rồi tính → rất kém hiệu quả
    → trong production với triệu đơn sẽ timeout
  → Redis cache kết quả aggregate sẵn → nhanh nhất
  → MongoDB aggregate scan collection nhưng kết quả chính xác
  → Kết luận: MongoDB tính → cache vào Redis → serve từ Redis
    """)


# ============================================================
# BENCHMARK 8: DOANH THU THEO KHOẢNG THỜI GIAN
# Chức năng: show_income_by_interval(restaurant)
# So sánh: MongoDB aggregate vs Redis vs Cassandra
# ============================================================
def bench8_income_by_interval():
    divider("BENCHMARK 8: Doanh thu theo khoảng thời gian")
    print("  Chức năng: show_income_by_interval(restaurant)\n")

    now        = datetime.now(timezone.utc)
    start_date = now - timedelta(days=30)
    end_date   = now
    cache_key  = f"stats:income:{rid_str}:30d"

    # Chuẩn bị Redis
    agg = list(db.orders.aggregate([
        {"$match": {
            "restaurant_id": rid,
            "current_status": "COMPLETED",
            "created_at": {"$gte": start_date, "$lte": end_date}
        }},
        {"$group": {
            "_id": {"$dateToString": {"format": "%Y-%m-%d", "date": "$created_at"}},
            "daily_income": {"$sum": "$total_amount"},
            "count": {"$sum": 1}
        }},
        {"$sort": {"_id": 1}}
    ]))
    r.set(cache_key, json.dumps(agg, default=str), ex=300)

    def read_mongodb():
        list(db.orders.aggregate([
            {"$match": {
                "restaurant_id": rid,
                "current_status": "COMPLETED",
                "created_at": {"$gte": start_date, "$lte": end_date}
            }},
            {"$group": {
                "_id": {"$dateToString": {"format": "%Y-%m-%d", "date": "$created_at"}},
                "daily_income": {"$sum": "$total_amount"},
                "count": {"$sum": 1}
            }},
            {"$sort": {"_id": 1}}
        ]))

    def read_redis():
        data = r.get(cache_key)
        json.loads(data)

    def read_cassandra():
        # Cassandra có clustering key created_at → hỗ trợ range query theo thời gian
        # nhưng partition key là customer_id, không phải restaurant_id
        # Workaround: query theo customer + time range, filter restaurant trên Python
        rows = list(cass.execute("""
            SELECT created_at, total_amount, current_status, restaurant_name
            FROM order_history_by_customer
            WHERE customer_id = %s
              AND created_at >= %s
              AND created_at <= %s
        """, (cid_cass, start_date, end_date)))
        # Group by date trên Python
        from collections import defaultdict
        daily = defaultdict(int)
        for row in rows:
            if (row.current_status == "COMPLETED"
                    and row.restaurant_name == sample_restaurant.get("name")):
                day = row.created_at.strftime("%Y-%m-%d")
                daily[day] += row.total_amount or 0

    results = {
        "MongoDB aggregate":      measure(read_mongodb,   repeat=100),
        "Redis cache":            measure(read_redis,     repeat=100),
        "Cassandra (workaround)": measure(read_cassandra, repeat=100),
    }

    for label, stats in results.items():
        print_result(label, stats)
    print_winner(results)
    print(f"""
  Phân tích:
  → Cassandra hỗ trợ range query theo thời gian (clustering key)
    nhưng partition key là customer_id → phải query từng customer
    → không scalable để tổng hợp doanh thu theo restaurant
  → Redis nhanh nhất vì kết quả group by date đã tính sẵn
  → MongoDB aggregate chính xác, hỗ trợ query theo restaurant_id
  → Kết luận: MongoDB tính → cache Redis → TTL dài vì data cũ không đổi
    """)


# ============================================================
# TỔNG KẾT
# ============================================================
def print_summary():
    divider("TỔNG KẾT — RESTAURANT MANAGEMENT BENCHMARK")
    print(f"""
  ┌───────────────────────┬──────────┬───────────┬────────────┬──────────────┐
  │ Chức năng             │ MongoDB  │ Redis     │ Cassandra  │ Nên dùng     │
  ├───────────────────────┼──────────┼──────────-┼────────────┼──────────────┤
  │ Danh sách nhà hàng   │ Chậm hơn │ Nhanh hơn │ ❌ Không   │ Redis        │
  │ Xem menu              │ Chậm hơn │ Nhanh hơn │ ❌ Không   │ Redis        │
  │ Thêm món              │ Ghi chính│ Invalidate│ ✅ Log     │ Cả 3         │
  │ Xem tất cả đơn        │ Phù hợp  │ TTL ngắn  │ ❌ Không   │ MongoDB      │
  │ Lọc đơn theo status   │ Phù hợp  │ TTL ngắn  │ ❌ Không   │ MongoDB      │
  │ Cập nhật status       │ Ghi chính│ Sync ngay │ ✅ Log     │ Cả 3         │
  │ Tổng doanh thu        │ Aggregate│ Nhanh hơn │ ❌ Không   │ MongoDB→Redis│
  │ Doanh thu theo ngày   │ Aggregate│ Nhanh hơn │ ❌ Không   │ MongoDB→Redis│
  └───────────────────────┴──────────┴───────────┴────────────┴──────────────┘

  Cassandra phù hợp trong project này cho:
  ✅ Ghi activity log (thêm món, đổi status) — append-only, volume lớn
  ✅ Lịch sử đơn của customer — partition theo customer_id
  ❌ Query theo restaurant_id — sai partition key
  ❌ Aggregate SUM/GROUP BY — Cassandra không hỗ trợ tốt
  ❌ Danh sách entity (nhà hàng, menu) — không phải use case của Cassandra

  Kết luận:
  → MongoDB: source of truth, query linh hoạt theo restaurant
  → Redis: cache đọc nhiều (menu, danh sách, thống kê)
  → Cassandra: log sự kiện ghi (status changes, menu changes)
    """)


# ============================================================
# MAIN
# ============================================================
if __name__ == "__main__":
    print("\n RESTAURANT MANAGEMENT BENCHMARK")
    print("   So sánh MongoDB vs Redis vs Cassandra")
    print("   cho các chức năng quản lý nhà hàng\n")

    bench1_list_restaurants()
    bench2_view_menu()
    bench3_add_dish()
    bench4_view_all_orders()
    bench5_filter_by_status()
    bench6_update_order_status()
    bench7_total_income()
    bench8_income_by_interval()
    print_summary()

    print("\n✅ Benchmark hoàn tất!\n")