"""
gendata.py (enhanced)
=====================
Tăng volume data để benchmark có ý nghĩa:
  - customers:   300   (tăng từ 150)
  - restaurants:  50   (giữ nguyên)
  - menu_items:  ~400  (giữ nguyên tỉ lệ)
  - orders:    10 000  (tăng từ 600  → benchmark target)
  - reviews:    ~4 500 (45% completed orders)
  - Cassandra activity: tất cả orders (thay vì sample 400)

Tối ưu tốc độ:
  - MongoDB  : insert_many theo batch 500
  - Cassandra: execute_concurrent (parallel async)
  - Progress : in % để biết tiến trình
"""

from faker import Faker
from pymongo import MongoClient
from cassandra.cluster import Cluster
from cassandra import ConsistencyLevel
from cassandra.concurrent import execute_concurrent_with_args
import redis
import random
import uuid
from datetime import datetime, timedelta
from bson import ObjectId

fake = Faker("vi_VN")

mongo = MongoClient("mongodb://admin:admin123@localhost:27017")
db    = mongo["food_delivery"]
cass  = Cluster(["localhost"], port=9042).connect("food_delivery")
r     = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)

# ============================================================
# CONFIG — chỉnh ở đây nếu muốn nhiều/ít hơn
# ============================================================
N_CUSTOMERS   = 300
N_RESTAURANTS = 50
N_ORDERS      = 13_000  # ← số đơn hàng target
BATCH_SIZE    = 500       # MongoDB batch insert size
CASS_CONCURR  = 200       # Cassandra concurrent statements

# ============================================================
# DATA CONSTANTS
# ============================================================
CUISINE_MENU = {
    "Bun Bo":  ["Bun Bo Hue dac biet", "Bun Bo gio heo", "Bun Bo trung"],
    "Com Tam": ["Com tam suon bi cha", "Com tam suon nuong", "Com tam bi trung"],
    "Pho":     ["Pho bo tai", "Pho bo vien", "Pho ga"],
    "Banh Mi": ["Banh mi thit nguoi", "Banh mi dac biet", "Banh mi trung"],
    "Pizza":   ["Pizza pho mai", "Pizza hai san", "Pizza thit nguoi"],
    "Sushi":   ["Sushi ca hoi", "Sushi ca ngu", "Sashimi tong hop"],
}
DRINKS    = ["Tra sua", "Nuoc cam", "Ca phe sua da", "Tra da", "Nuoc ep"]
DESSERTS  = ["Che thai", "Banh flan", "Kem chuoi"]
SNACKS    = ["Goi cuon", "Cha gio", "Banh trang nuong"]
DISTRICTS = ["Quan 1", "Quan 3", "Binh Thanh", "Thu Duc", "Go Vap"]
STATUS_FLOW = ["PLACED","CONFIRMED","PREPARING","PICKED_UP","DELIVERING","COMPLETED"]


def progress(current, total, label=""):
    pct  = current / total * 100
    done = int(pct / 5)
    bar  = "█" * done + "░" * (20 - done)
    print(f"\r  [{bar}] {pct:5.1f}%  {label}", end="", flush=True)


# ============================================================
# CUSTOMERS
# ============================================================
def gen_customers(n=N_CUSTOMERS):
    docs = []
    for _ in range(n):
        docs.append({
            "_id":          ObjectId(),
            "full_name":    fake.name(),
            "phone":        fake.phone_number()[:15],
            "email":        fake.email(),
            "gender":       random.choice(["male", "female"]),
            "birth_year":   random.randint(1980, 2003),
            "addresses": [{
                "label":      "Home",
                "street":     fake.street_address(),
                "district":   random.choice(DISTRICTS),
                "city":       "Ho Chi Minh",
                "lat":        round(random.uniform(10.70, 10.85), 6),
                "lng":        round(random.uniform(106.60, 106.80), 6),
                "is_default": True,
            }],
            "registered_at": fake.date_time_between("-2y", "now"),
            "status":        random.choices(["active","blocked"], weights=[95, 5])[0],
        })
    db.customers.drop()
    db.customers.insert_many(docs)
    print(f"✅ MongoDB customers: {len(docs)}")
    return docs


# ============================================================
# RESTAURANTS
# ============================================================
def gen_restaurants(n=N_RESTAURANTS):
    docs = []
    for _ in range(n):
        cuisine = random.choice(list(CUISINE_MENU.keys()))
        docs.append({
            "_id":          ObjectId(),
            "name":         f"{cuisine} {fake.last_name()}",
            "cuisine_type": cuisine,
            "phone":        fake.phone_number()[:15],
            "street":       fake.street_address(),
            "district":     random.choice(DISTRICTS),
            "city":         "Ho Chi Minh",
            "lat":          round(random.uniform(10.70, 10.85), 6),
            "lng":          round(random.uniform(106.60, 106.80), 6),
            "open_time":    "07:00",
            "close_time":   "22:00",
            "is_active":    random.choices([True, False], weights=[90, 10])[0],
            "avg_rating":   0.0,
            "total_reviews":0,
            "opened_at":    fake.date_time_between("-3y", "-6m"),
        })
    db.restaurants.drop()
    db.restaurants.insert_many(docs)
    print(f"✅ MongoDB restaurants: {len(docs)}")
    return docs


# ============================================================
# MENU ITEMS
# ============================================================
def gen_menu_items(restaurants):
    all_items = []
    rest_menu = {}
    for rest in restaurants:
        r_items  = []
        cuisine  = rest["cuisine_type"]
        for name in CUISINE_MENU[cuisine]:
            item = {
                "_id":          ObjectId(),
                "restaurant_id": rest["_id"],
                "name":         name,
                "category":     "main",
                "price":        random.choice([45,55,65,75,85]) * 1000,
                "is_available": random.choices([True, False], [90, 10])[0],
                "description":  fake.sentence(8),
                "updated_at":   datetime.now(),
            }
            all_items.append(item); r_items.append(item)
        for name in random.sample(DRINKS, 3):
            item = {
                "_id":          ObjectId(),
                "restaurant_id": rest["_id"],
                "name":         name,
                "category":     "drink",
                "price":        random.choice([20,25,30,35]) * 1000,
                "is_available": True,
                "description":  "",
                "updated_at":   datetime.now(),
            }
            all_items.append(item); r_items.append(item)
        for name in random.sample(DESSERTS, 2):
            item = {
                "_id":          ObjectId(),
                "restaurant_id": rest["_id"],
                "name":         name,
                "category":     "dessert",
                "price":        random.choice([20,25,30]) * 1000,
                "is_available": True,
                "description":  "",
                "updated_at":   datetime.now(),
            }
            all_items.append(item); r_items.append(item)
        rest_menu[str(rest["_id"])] = r_items

    db.menu_items.drop()
    db.menu_items.insert_many(all_items)
    print(f"✅ MongoDB menu_items: {len(all_items)}")
    return rest_menu


# ============================================================
# ORDERS  — batch insert, progress bar
# ============================================================
def gen_orders_mongo(customers, restaurants, rest_menu, n=N_ORDERS):
    print(f"\n  Generating {n:,} orders...")
    batch   = []
    orders  = []
    inserted = 0

    for i in range(n):
        cust  = random.choice(customers)
        rest  = random.choice(restaurants)
        addr  = cust["addresses"][0]
        avail = [item for item in rest_menu[str(rest["_id"])] if item["is_available"]]
        if not avail:
            continue

        chosen   = random.sample(avail, k=random.randint(1, min(4, len(avail))))
        subtotal = 0
        items    = []
        for item in chosen:
            qty  = random.randint(1, 3)
            lt   = item["price"] * qty
            subtotal += lt
            items.append({
                "menu_item_id": item["_id"],
                "name":         item["name"],
                "category":     item["category"],
                "quantity":     qty,
                "unit_price":   item["price"],
                "line_total":   lt,
            })

        fee      = 15_000
        discount = random.choice([0, 0, 0, 10_000, 20_000])
        total    = subtotal + fee - discount
        is_cancel = random.random() < 0.08
        created   = fake.date_time_between("-365d", "-1d")  # 1 năm để data phong phú hơn

        if is_cancel:
            history = [
                {"status": "PLACED",    "timestamp": created},
                {"status": "CANCELLED", "timestamp": created + timedelta(minutes=3)},
            ]
            status = "CANCELLED"
            reason = random.choice(["Khach hang huy", "Nha hang dong cua", "Het mon"])
        else:
            times = [created]
            for d in [2, 3, 15, 5, 15]:
                times.append(times[-1] + timedelta(minutes=d + random.randint(-1, 3)))
            history = [{"status": s, "timestamp": t} for s, t in zip(STATUS_FLOW, times)]
            status  = "COMPLETED"
            reason  = None

        doc = {
            "_id":               ObjectId(),
            "customer_id":       cust["_id"],
            "restaurant_id":     rest["_id"],
            "delivery_street":   addr["street"],
            "delivery_district": addr["district"],
            "delivery_city":     addr["city"],
            "items":             items,
            "status_history":    history,
            "current_status":    status,
            "payment_method":    random.choice(["CASH","MOMO","ZALOPAY","CARD"]),
            "items_subtotal":    subtotal,
            "delivery_fee":      fee,
            "discount_amount":   discount,
            "total_amount":      total,
            "cancel_reason":     reason,
            "created_at":        created,
            "updated_at":        history[-1]["timestamp"],
        }
        batch.append(doc)
        orders.append(doc)

        # Batch insert mỗi BATCH_SIZE docs
        if len(batch) >= BATCH_SIZE:
            db.orders.insert_many(batch, ordered=False)
            inserted += len(batch)
            batch = []
            progress(inserted, n, f"{inserted:,}/{n:,} orders")

    # Insert phần còn lại
    if batch:
        db.orders.insert_many(batch, ordered=False)
        inserted += len(batch)

    print(f"\n✅ MongoDB orders: {inserted:,}")
    return orders


# ============================================================
# REVIEWS — batch insert
# ============================================================
def gen_reviews(completed_orders):
    print(f"\n  Generating reviews for {len(completed_orders):,} completed orders...")
    sample   = random.sample(completed_orders, k=int(len(completed_orders) * 0.45))
    batch    = []
    inserted = 0

    for i, o in enumerate(sample):
        batch.append({
            "_id":                   ObjectId(),
            "order_id":              o["_id"],
            "customer_id":           o["customer_id"],
            "restaurant_id":         o["restaurant_id"],
            "overall_rating":        random.randint(3, 5),
            "food_quality_rating":   random.randint(3, 5),
            "delivery_speed_rating": random.randint(2, 5),
            "comment":               fake.sentence(10),
            "has_photo":             random.random() < 0.2,
            "item_ratings": [
                {
                    "menu_item_id": it["menu_item_id"],
                    "name":         it["name"],
                    "rating":       random.randint(3, 5),
                }
                for it in o["items"]
            ],
            "created_at": o["updated_at"] + timedelta(minutes=random.randint(5, 60)),
        })

        if len(batch) >= BATCH_SIZE:
            db.reviews.insert_many(batch, ordered=False)
            inserted += len(batch)
            batch = []
            progress(inserted, len(sample), f"{inserted:,}/{len(sample):,} reviews")

    if batch:
        db.reviews.insert_many(batch, ordered=False)
        inserted += len(batch)

    print(f"\n✅ MongoDB reviews: {inserted:,}")
    return inserted


def update_ratings():
    for doc in db.reviews.aggregate([
        {"$group": {
            "_id": "$restaurant_id",
            "avg": {"$avg": "$overall_rating"},
            "cnt": {"$sum": 1},
        }}
    ]):
        db.restaurants.update_one(
            {"_id": doc["_id"]},
            {"$set": {"avg_rating": round(doc["avg"], 2), "total_reviews": doc["cnt"]}},
        )
    print("✅ MongoDB avg_rating updated")


def gen_daily_stats():
    pipeline = [
        {"$group": {
            "_id": {
                "rid":  "$restaurant_id",
                "date": {"$dateToString": {"format": "%Y-%m-%d", "date": "$created_at"}},
            },
            "completed": {"$sum": {"$cond": [{"$eq": ["$current_status", "COMPLETED"]}, 1, 0]}},
            "cancelled": {"$sum": {"$cond": [{"$eq": ["$current_status", "CANCELLED"]}, 1, 0]}},
            "revenue":   {"$sum": {"$cond": [{"$eq": ["$current_status", "COMPLETED"]}, "$total_amount", 0]}},
            "avg_val":   {"$avg": {"$cond": [{"$eq": ["$current_status", "COMPLETED"]}, "$total_amount", None]}},
        }},
        {"$project": {
            "_id":              0,
            "restaurant_id":    "$_id.rid",
            "date":             "$_id.date",
            "orders_completed": "$completed",
            "orders_cancelled": "$cancelled",
            "gross_revenue":    {"$round": ["$revenue", 0]},
            "avg_order_value":  {"$round": ["$avg_val",  0]},
        }},
    ]
    stats = list(db.orders.aggregate(pipeline))
    for s in stats:
        s["_id"] = ObjectId()
    db.restaurant_daily_stats.drop()
    if stats:
        db.restaurant_daily_stats.insert_many(stats)
    print(f"✅ MongoDB restaurant_daily_stats: {len(stats):,} records")


# ============================================================
# CASSANDRA ACTIVITY — execute_concurrent (toàn bộ orders)
# ============================================================
def gen_cassandra_activity(orders):
    print(f"\n  Writing Cassandra activity for {len(orders):,} orders (concurrent)...")
    stmt = cass.prepare("""
        INSERT INTO customer_activity
        (customer_id, timestamp, event_id, event_type, restaurant_id, menu_item_id, search_keyword)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """)
    stmt.consistency_level = ConsistencyLevel.ONE

    event_types = ["VIEW_RESTAURANT", "VIEW_ITEM", "ADD_TO_CART", "SEARCH"]
    params_list = []

    for o in orders:
        cid = uuid.uuid5(uuid.NAMESPACE_OID, str(o["customer_id"]))
        rid = uuid.uuid5(uuid.NAMESPACE_OID, str(o["restaurant_id"]))
        base = o["created_at"]

        # Pre-order browsing events (2–5 events per order)
        for _ in range(random.randint(2, 5)):
            mid = uuid.uuid5(uuid.NAMESPACE_OID, str(random.choice(o["items"])["menu_item_id"]))
            ts  = base - timedelta(minutes=random.randint(1, 20))
            params_list.append((cid, ts, uuid.uuid4(), random.choice(event_types), rid, mid, None))

        # ORDER_PLACED event
        params_list.append((cid, base, uuid.uuid4(), "ORDER_PLACED", rid, None, None))

    # Batch execute với concurrency cao
    total   = len(params_list)
    written = 0
    chunk   = CASS_CONCURR * 5  # xử lý 1000 records mỗi lượt

    for i in range(0, total, chunk):
        batch = params_list[i:i + chunk]
        results = execute_concurrent_with_args(
            cass, stmt, batch, concurrency=CASS_CONCURR
        )
        written += len(batch)
        progress(written, total, f"{written:,}/{total:,} activity events")

    print(f"\n✅ Cassandra customer_activity: {written:,} events")


# ============================================================
# CASSANDRA ORDER HISTORY — execute_concurrent
# ============================================================
def gen_cassandra_order_history(orders, restaurants):
    print(f"\n  Writing Cassandra order_history for {len(orders):,} orders (concurrent)...")
    rest_map = {str(r["_id"]): r["name"] for r in restaurants}
    stmt = cass.prepare("""
        INSERT INTO order_history_by_customer
        (customer_id, created_at, order_id, restaurant_name, current_status,
         total_amount, payment_method, item_summary)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """)
    stmt.consistency_level = ConsistencyLevel.ONE

    params_list = []
    for o in orders:
        cid     = uuid.uuid5(uuid.NAMESPACE_OID, str(o["customer_id"]))
        oid     = uuid.uuid5(uuid.NAMESPACE_OID, str(o["_id"]))
        rname   = rest_map.get(str(o["restaurant_id"]), "Unknown")
        summary = ", ".join([f"{i['name']} x{i['quantity']}" for i in o["items"][:3]])
        params_list.append((
            cid, o["created_at"], oid, rname,
            o["current_status"], o["total_amount"],
            o["payment_method"], summary,
        ))

    total   = len(params_list)
    written = 0
    chunk   = CASS_CONCURR * 5

    for i in range(0, total, chunk):
        batch = params_list[i:i + chunk]
        execute_concurrent_with_args(cass, stmt, batch, concurrency=CASS_CONCURR)
        written += len(batch)
        progress(written, total, f"{written:,}/{total:,} order history records")

    print(f"\n✅ Cassandra order_history: {written:,} records")


# ============================================================
# REDIS
# ============================================================
def gen_redis_data(orders, restaurants):
    # Cache active order statuses
    active = [o for o in orders if o["current_status"] not in ["COMPLETED", "CANCELLED"]]
    pipe   = r.pipeline()
    for o in active[:200]:   # cache tối đa 200 active orders
        key = f"order:status:{str(o['_id'])}"
        pipe.hset(key, mapping={
            "status":        o["current_status"],
            "updated_at":    o["updated_at"].isoformat(),
            "customer_id":   str(o["customer_id"]),
            "restaurant_id": str(o["restaurant_id"]),
        })
        pipe.expire(key, 7200)
    pipe.execute()

    # Restaurant ranking sorted set
    r.delete("ranking:restaurants")
    pipe2 = r.pipeline()
    for rest in db.restaurants.find({}, {"avg_rating": 1}):
        pipe2.zadd("ranking:restaurants", {str(rest["_id"]): rest["avg_rating"]})
    pipe2.execute()

    print(f"✅ Redis: {len(active[:200])} order status cached, restaurant ranking set")


# ============================================================
# MAIN
# ============================================================
if __name__ == "__main__":
    import time
    t0 = time.perf_counter()

    print("🚀 Bắt đầu gen data...\n")
    print(f"  Target: {N_CUSTOMERS} customers | {N_RESTAURANTS} restaurants | {N_ORDERS:,} orders\n")

    customers   = gen_customers(N_CUSTOMERS)
    restaurants = gen_restaurants(N_RESTAURANTS)
    rest_menu   = gen_menu_items(restaurants)

    db.orders.drop()
    orders = gen_orders_mongo(customers, restaurants, rest_menu, N_ORDERS)

    completed = [o for o in orders if o["current_status"] == "COMPLETED"]
    print(f"  Completed orders: {len(completed):,} ({len(completed)/len(orders)*100:.0f}%)")

    db.reviews.drop()
    gen_reviews(completed)
    update_ratings()
    gen_daily_stats()

    gen_cassandra_activity(orders)
    gen_cassandra_order_history(orders, restaurants)
    gen_redis_data(orders, restaurants)

    # Summary
    elapsed = time.perf_counter() - t0
    print(f"\n{'='*50}")
    print(f"🎉 HOÀN TẤT trong {elapsed:.1f}s")
    print(f"{'='*50}")
    print(f"  MongoDB:")
    print(f"    customers            : {db.customers.count_documents({}):>8,}")
    print(f"    restaurants          : {db.restaurants.count_documents({}):>8,}")
    print(f"    menu_items           : {db.menu_items.count_documents({}):>8,}")
    print(f"    orders               : {db.orders.count_documents({}):>8,}")
    print(f"    reviews              : {db.reviews.count_documents({}):>8,}")
    print(f"    restaurant_daily_stats: {db.restaurant_daily_stats.count_documents({}):>7,}")
    print(f"  Cassandra + Redis: đã insert ✅")