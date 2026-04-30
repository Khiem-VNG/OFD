from faker import Faker
from pymongo import MongoClient
from cassandra.cluster import Cluster
import redis
import random
import uuid
import json
from datetime import datetime, timedelta
from bson import ObjectId

fake = Faker("vi_VN")

mongo  = MongoClient("mongodb://admin:admin123@localhost:27017")
db     = mongo["food_delivery"]
cass   = Cluster(["localhost"], port=9042).connect("food_delivery")
r      = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)

CUISINE_MENU = {
    "Bun Bo":  ["Bun Bo Hue dac biet", "Bun Bo gio heo", "Bun Bo trung"],
    "Com Tam": ["Com tam suon bi cha", "Com tam suon nuong", "Com tam bi trung"],
    "Pho":     ["Pho bo tai", "Pho bo vien", "Pho ga"],
    "Banh Mi": ["Banh mi thit nguoi", "Banh mi dac biet", "Banh mi trung"],
    "Pizza":   ["Pizza pho mai", "Pizza hai san", "Pizza thit nguoi"],
    "Sushi":   ["Sushi ca hoi", "Sushi ca ngu", "Sashimi tong hop"],
}

DRINKS   = ["Tra sua", "Nuoc cam", "Ca phe sua da", "Tra da", "Nuoc ep"]
DESSERTS = ["Che thai", "Banh flan", "Kem chuoi"]
SNACKS   = ["Goi cuon", "Cha gio", "Banh trang nuong"]
DISTRICTS = ["Quan 1", "Quan 3", "Binh Thanh", "Thu Duc", "Go Vap"]
STATUS_FLOW = ["PLACED","CONFIRMED","PREPARING","PICKED_UP","DELIVERING","COMPLETED"]


def gen_customers(n=150):
    docs = []
    for _ in range(n):
        docs.append({
            "_id": ObjectId(),
            "full_name":     fake.name(),
            "phone":         fake.phone_number()[:15],
            "email":         fake.email(),
            "gender":        random.choice(["male", "female"]),
            "birth_year":    random.randint(1980, 2003),
            "addresses": [{
                "label":      "Home",
                "street":     fake.street_address(),
                "district":   random.choice(DISTRICTS),
                "city":       "Ho Chi Minh",
                "lat":        round(random.uniform(10.70, 10.85), 6),
                "lng":        round(random.uniform(106.60, 106.80), 6),
                "is_default": True
            }],
            "registered_at": fake.date_time_between("-2y", "now"),
            "status":        random.choices(["active","blocked"], weights=[95,5])[0]
        })
    db.customers.drop()
    db.customers.insert_many(docs)
    print(f"✅ MongoDB customers: {len(docs)}")
    return docs


def gen_restaurants(n=50):
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
            "is_active":    random.choices([True,False], weights=[90,10])[0],
            "avg_rating":   0.0,
            "total_reviews":0,
            "opened_at":    fake.date_time_between("-3y", "-6m")
        })
    db.restaurants.drop()
    db.restaurants.insert_many(docs)
    print(f"✅ MongoDB restaurants: {len(docs)}")
    return docs


def gen_menu_items(restaurants):
    all_items = []
    rest_menu = {}
    for rest in restaurants:
        r_items = []
        cuisine = rest["cuisine_type"]
        for name in CUISINE_MENU[cuisine]:
            item = {"_id": ObjectId(), "restaurant_id": rest["_id"], "name": name,
                    "category": "main", "price": random.choice([45,55,65,75,85])*1000,
                    "is_available": random.choices([True,False],[90,10])[0],
                    "description": fake.sentence(8), "updated_at": datetime.now()}
            all_items.append(item); r_items.append(item)
        for name in random.sample(DRINKS, 3):
            item = {"_id": ObjectId(), "restaurant_id": rest["_id"], "name": name,
                    "category": "drink", "price": random.choice([20,25,30,35])*1000,
                    "is_available": True, "description": "", "updated_at": datetime.now()}
            all_items.append(item); r_items.append(item)
        for name in random.sample(DESSERTS, 2):
            item = {"_id": ObjectId(), "restaurant_id": rest["_id"], "name": name,
                    "category": "dessert", "price": random.choice([20,25,30])*1000,
                    "is_available": True, "description": "", "updated_at": datetime.now()}
            all_items.append(item); r_items.append(item)
        rest_menu[str(rest["_id"])] = r_items
    db.menu_items.drop()
    db.menu_items.insert_many(all_items)
    print(f"✅ MongoDB menu_items: {len(all_items)}")
    return rest_menu


def gen_orders_mongo(customers, restaurants, rest_menu, n=600):
    orders = []
    for _ in range(n):
        cust = random.choice(customers)
        rest = random.choice(restaurants)
        addr = cust["addresses"][0]
        avail = [i for i in rest_menu[str(rest["_id"])] if i["is_available"]]
        if not avail: continue
        chosen = random.sample(avail, k=random.randint(1, min(4, len(avail))))
        subtotal = 0
        items = []
        for item in chosen:
            qty = random.randint(1, 3)
            lt  = item["price"] * qty
            subtotal += lt
            items.append({"menu_item_id": item["_id"], "name": item["name"],
                          "category": item["category"], "quantity": qty,
                          "unit_price": item["price"], "line_total": lt})
        fee      = 15000
        discount = random.choice([0,0,0,10000,20000])
        total    = subtotal + fee - discount
        is_cancel = random.random() < 0.08
        created   = fake.date_time_between("-6m", "now")
        if is_cancel:
            history = [{"status":"PLACED","timestamp":created},
                       {"status":"CANCELLED","timestamp":created+timedelta(minutes=3)}]
            status = "CANCELLED"
            reason = random.choice(["Khach hang huy","Nha hang dong cua","Het mon"])
        else:
            times = [created]
            for d in [2,3,15,5,15]:
                times.append(times[-1]+timedelta(minutes=d+random.randint(-1,3)))
            history = [{"status":s,"timestamp":t} for s,t in zip(STATUS_FLOW,times)]
            status = "COMPLETED"
            reason = None
        orders.append({
            "_id": ObjectId(), "customer_id": cust["_id"], "restaurant_id": rest["_id"],
            "delivery_street": addr["street"], "delivery_district": addr["district"],
            "delivery_city": addr["city"], "items": items,
            "status_history": history, "current_status": status,
            "payment_method": random.choice(["CASH","MOMO","ZALOPAY","CARD"]),
            "items_subtotal": subtotal, "delivery_fee": fee,
            "discount_amount": discount, "total_amount": total,
            "cancel_reason": reason, "created_at": created,
            "updated_at": history[-1]["timestamp"]
        })
    db.orders.drop()
    db.orders.insert_many(orders)
    print(f"✅ MongoDB orders: {len(orders)}")
    return orders


def gen_reviews(completed_orders):
    reviews = []
    sampled = random.sample(completed_orders, k=int(len(completed_orders)*0.45))
    for o in sampled:
        reviews.append({
            "_id": ObjectId(), "order_id": o["_id"],
            "customer_id": o["customer_id"], "restaurant_id": o["restaurant_id"],
            "overall_rating": random.randint(3,5),
            "food_quality_rating": random.randint(3,5),
            "delivery_speed_rating": random.randint(2,5),
            "comment": fake.sentence(10), "has_photo": random.random()<0.2,
            "item_ratings": [{"menu_item_id": i["menu_item_id"], "name": i["name"],
                               "rating": random.randint(3,5)} for i in o["items"]],
            "created_at": o["updated_at"]+timedelta(minutes=random.randint(5,60))
        })
    db.reviews.drop()
    db.reviews.insert_many(reviews)
    print(f"✅ MongoDB reviews: {len(reviews)}")
    return reviews


def update_ratings():
    for doc in db.reviews.aggregate([{"$group": {
        "_id": "$restaurant_id", "avg": {"$avg":"$overall_rating"}, "cnt":{"$sum":1}}}]):
        db.restaurants.update_one({"_id":doc["_id"]},
            {"$set":{"avg_rating":round(doc["avg"],2),"total_reviews":doc["cnt"]}})
    print("✅ MongoDB avg_rating updated")


def gen_daily_stats():
    pipeline = [
        {"$group": {"_id": {"rid":"$restaurant_id",
                            "date":{"$dateToString":{"format":"%Y-%m-%d","date":"$created_at"}}},
                    "completed":{"$sum":{"$cond":[{"$eq":["$current_status","COMPLETED"]},1,0]}},
                    "cancelled":{"$sum":{"$cond":[{"$eq":["$current_status","CANCELLED"]},1,0]}},
                    "revenue":  {"$sum":{"$cond":[{"$eq":["$current_status","COMPLETED"]},"$total_amount",0]}},
                    "avg_val":  {"$avg":{"$cond":[{"$eq":["$current_status","COMPLETED"]},"$total_amount",None]}}}},
        {"$project":{"_id":0,"restaurant_id":"$_id.rid","date":"$_id.date",
                     "orders_completed":"$completed","orders_cancelled":"$cancelled",
                     "gross_revenue":{"$round":["$revenue",0]},
                     "avg_order_value":{"$round":["$avg_val",0]}}}
    ]
    stats = list(db.orders.aggregate(pipeline))
    for s in stats: s["_id"] = ObjectId()
    db.restaurant_daily_stats.drop()
    if stats: db.restaurant_daily_stats.insert_many(stats)
    print(f"✅ MongoDB restaurant_daily_stats: {len(stats)}")


def gen_cassandra_activity(orders):
    insert_activity = cass.prepare("""
        INSERT INTO customer_activity
        (customer_id, timestamp, event_id, event_type, restaurant_id, menu_item_id, search_keyword)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """)
    event_types = ["VIEW_RESTAURANT","VIEW_ITEM","ADD_TO_CART","SEARCH"]
    count = 0
    sampled = random.sample(orders, k=min(len(orders), 400))
    for o in sampled:
        cid = uuid.uuid5(uuid.NAMESPACE_OID, str(o["customer_id"]))
        rid = uuid.uuid5(uuid.NAMESPACE_OID, str(o["restaurant_id"]))
        base = o["created_at"]
        for _ in range(random.randint(2, 5)):
            mid = uuid.uuid5(uuid.NAMESPACE_OID, str(random.choice(o["items"])["menu_item_id"]))
            ts  = base - timedelta(minutes=random.randint(1,20))
            cass.execute(insert_activity, (cid, ts, uuid.uuid4(),
                         random.choice(event_types), rid, mid, None))
            count += 1
        cass.execute(insert_activity, (cid, base, uuid.uuid4(),
                     "ORDER_PLACED", rid, None, None))
        count += 1
    print(f"✅ Cassandra customer_activity: {count} events")


def gen_cassandra_order_history(orders, restaurants):
    rest_map = {str(r["_id"]): r["name"] for r in restaurants}
    insert_history = cass.prepare("""
        INSERT INTO order_history_by_customer
        (customer_id, created_at, order_id, restaurant_name, current_status,
         total_amount, payment_method, item_summary)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """)
    count = 0
    for o in orders:
        cid = uuid.uuid5(uuid.NAMESPACE_OID, str(o["customer_id"]))
        oid = uuid.uuid5(uuid.NAMESPACE_OID, str(o["_id"]))
        rname   = rest_map.get(str(o["restaurant_id"]), "Unknown")
        summary = ", ".join([f"{i['name']} x{i['quantity']}" for i in o["items"][:3]])
        cass.execute(insert_history, (
            cid, o["created_at"], oid, rname,
            o["current_status"], o["total_amount"],
            o["payment_method"], summary
        ))
        count += 1
    print(f"✅ Cassandra order_history: {count} records")


def gen_redis_data(orders, restaurants):
    pipe = r.pipeline()
    active = [o for o in orders if o["current_status"] not in ["COMPLETED","CANCELLED"]]
    for o in active[:50]:
        key = f"order:status:{str(o['_id'])}"
        pipe.hset(key, mapping={
            "status":        o["current_status"],
            "updated_at":    o["updated_at"].isoformat(),
            "customer_id":   str(o["customer_id"]),
            "restaurant_id": str(o["restaurant_id"])
        })
        pipe.expire(key, 7200)
    r.delete("ranking:restaurants")
    for rest in restaurants:
        pipe.zadd("ranking:restaurants", {str(rest["_id"]): rest["avg_rating"]})
    pipe.execute()
    print(f"✅ Redis: {len(active[:50])} order status cached, restaurant ranking set")


if __name__ == "__main__":
    print("🚀 Bắt đầu gen data...\n")
    customers   = gen_customers(150)
    restaurants = gen_restaurants(50)
    rest_menu   = gen_menu_items(restaurants)
    orders      = gen_orders_mongo(customers, restaurants, rest_menu, 600)
    completed   = [o for o in orders if o["current_status"]=="COMPLETED"]
    gen_reviews(completed)
    update_ratings()
    gen_daily_stats()
    gen_cassandra_activity(orders)
    gen_cassandra_order_history(orders, restaurants)
    gen_redis_data(orders, restaurants)
    total = db.customers.count_documents({}) + db.restaurants.count_documents({}) + \
            db.menu_items.count_documents({}) + db.orders.count_documents({}) + \
            db.reviews.count_documents({})
    print(f"\n🎉 Tổng MongoDB records: {total}")
    print("✅ Cassandra + Redis data đã được insert!")