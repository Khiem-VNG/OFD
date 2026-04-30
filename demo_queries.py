from pymongo import MongoClient
from cassandra.cluster import Cluster
import redis
import json
from bson import ObjectId
from datetime import datetime

db   = MongoClient("mongodb://admin:admin123@localhost:27017")["food_delivery"]
cass = Cluster(["localhost"], port=9042).connect("food_delivery")
r    = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)


def func1_get_restaurants_with_menu(district: str, category: str = None):
    print(f"\n{'='*60}")
    print(f"CHỨC NĂNG 1 [MongoDB]: Nhà hàng tại '{district}'" +
          (f" | Category: {category}" if category else ""))
    print('='*60)
    restaurants = list(db.restaurants.find(
        {"district": district, "is_active": True}
    ).sort("avg_rating", -1).limit(5))
    if not restaurants:
        print("  Không tìm thấy nhà hàng.")
        return
    for rest in restaurants:
        print(f"\n  🏪 {rest['name']} | ⭐ {rest['avg_rating']} ({rest['total_reviews']} reviews)")
        print(f"     📍 {rest['street']}, {rest['district']}")
        menu_query = {"restaurant_id": rest["_id"], "is_available": True}
        if category:
            menu_query["category"] = category
        menu = list(db.menu_items.find(menu_query,
                    {"name":1,"category":1,"price":1}).sort("category",1))
        print(f"     🍽️  Menu ({len(menu)} món):")
        for item in menu:
            print(f"        [{item['category']:7}] {item['name']:35} {item['price']:>8,.0f}đ")


def func2_restaurant_stats_by_month(year: int, month: int):
    print(f"\n{'='*60}")
    print(f"CHỨC NĂNG 2 [MongoDB]: Thống kê nhà hàng tháng {month}/{year}")
    print('='*60)
    start = datetime(year, month, 1)
    end   = datetime(year, month+1, 1) if month < 12 else datetime(year+1, 1, 1)
    pipeline = [
        {"$match": {"created_at":{"$gte":start,"$lt":end}, "current_status":"COMPLETED"}},
        {"$group": {"_id":"$restaurant_id", "total_orders":{"$sum":1},
                    "total_revenue":{"$sum":"$total_amount"},
                    "avg_order_value":{"$avg":"$total_amount"}}},
        {"$lookup": {"from":"restaurants","localField":"_id",
                     "foreignField":"_id","as":"info"}},
        {"$unwind": "$info"},
        {"$lookup": {"from":"reviews", "let":{"rid":"$_id"},
                     "pipeline":[
                         {"$match":{"$expr":{"$and":[
                             {"$eq":["$restaurant_id","$$rid"]},
                             {"$gte":["$created_at",start]},{"$lt":["$created_at",end]}]}}},
                         {"$group":{"_id":None,"avg":{"$avg":"$overall_rating"},"cnt":{"$sum":1}}}
                     ], "as":"month_reviews"}},
        {"$project": {"restaurant_name":"$info.name","district":"$info.district",
                      "total_orders":1,"total_revenue":{"$round":["$total_revenue",0]},
                      "avg_order_value":{"$round":["$avg_order_value",0]},
                      "month_avg_rating":{"$round":[{"$arrayElemAt":["$month_reviews.avg",0]},2]},
                      "month_reviews":{"$arrayElemAt":["$month_reviews.cnt",0]}}},
        {"$sort": {"total_revenue":-1}}, {"$limit": 10}
    ]
    results = list(db.orders.aggregate(pipeline))
    print(f"  {'Nhà hàng':<28} {'Quận':<12} {'Đơn':>5} {'Doanh thu':>14} {'⭐':>6}")
    print(f"  {'-'*70}")
    for r_doc in results:
        rating = f"{r_doc.get('month_avg_rating') or 0:.1f}"
        print(f"  {r_doc['restaurant_name']:<28} {r_doc['district']:<12} "
              f"{r_doc['total_orders']:>5} {r_doc['total_revenue']:>14,.0f}đ {rating:>6}")


def func3_customer_history_cassandra(customer_id_str: str):
    print(f"\n{'='*60}")
    print(f"CHỨC NĂNG 3 [Cassandra]: Lịch sử & Activity của customer")
    print('='*60)
    import uuid
    try:
        cid = uuid.UUID(customer_id_str)
    except ValueError:
        print("  Invalid UUID format")
        return
    rows = cass.execute("""
        SELECT order_id, created_at, restaurant_name, current_status,
               total_amount, payment_method, item_summary
        FROM order_history_by_customer
        WHERE customer_id = %s
        LIMIT 10
    """, (cid,))
    print(f"\n  📦 Lịch sử đơn hàng (Cassandra - 10 đơn gần nhất):")
    orders = list(rows)
    if not orders:
        print("  Không có đơn hàng")
    for o in orders:
        print(f"    [{o.current_status:12}] {o.restaurant_name} | "
              f"{o.total_amount:,.0f}đ | {o.created_at.strftime('%d/%m/%Y %H:%M')}")
        print(f"      Món: {o.item_summary}")
    activity = cass.execute("""
        SELECT event_type, timestamp, restaurant_id, menu_item_id
        FROM customer_activity
        WHERE customer_id = %s
        LIMIT 10
    """, (cid,))
    print(f"\n  📊 Activity log gần nhất (Cassandra):")
    events = list(activity)
    if not events:
        print("  Chưa có activity")
    for e in events:
        print(f"    {e.timestamp.strftime('%d/%m %H:%M')} | {e.event_type}")


def func4_redis_realtime():
    print(f"\n{'='*60}")
    print(f"CHỨC NĂNG 4 [Redis]: Cache & Real-time Order Tracking")
    print('='*60)
    print("\n  🏆 Top 5 nhà hàng theo rating (Redis Sorted Set):")
    top_restaurants = r.zrevrange("ranking:restaurants", 0, 4, withscores=True)
    if not top_restaurants:
        print("  Chưa có data trong Redis ranking")
    for i, (rid, score) in enumerate(top_restaurants, 1):
        rest_doc = db.restaurants.find_one({"_id": ObjectId(rid)}, {"name":1,"district":1})
        if rest_doc:
            print(f"    {i}. {rest_doc['name']} ({rest_doc['district']}) | ⭐ {score:.2f}")
    sample_rest = db.restaurants.find_one({"is_active": True})
    if sample_rest:
        rid_str   = str(sample_rest["_id"])
        cache_key = f"menu:{rid_str}"
        cached = r.get(cache_key)
        if not cached:
            print(f"\n  📭 Cache MISS cho {sample_rest['name']} → Đọc từ MongoDB...")
            items = list(db.menu_items.find(
                {"restaurant_id": sample_rest["_id"], "is_available": True},
                {"name":1,"category":1,"price":1,"_id":0}
            ))
            r.set(cache_key, json.dumps(items, default=str), ex=300)
            print(f"     → Cached {len(items)} món | TTL: 300s")
        else:
            print(f"\n  ✅ Cache HIT cho {sample_rest['name']}")
            items = json.loads(cached)
            print(f"     → {len(items)} món từ Redis (không cần query MongoDB)")
    print(f"\n  🚀 Order Status từ Redis (real-time cache):")
    order_keys = r.keys("order:status:*")[:3]
    if not order_keys:
        print("  Không có active orders trong cache")
    for key in order_keys:
        data = r.hgetall(key)
        ttl  = r.ttl(key)
        oid  = key.split(":")[-1]
        print(f"    Order {oid[:8]}... | Status: {data.get('status')} | TTL: {ttl}s")
    print(f"\n  🔐 Demo Session Management:")
    import uuid as uuid_mod
    session_id = str(uuid_mod.uuid4())
    r.hset(f"session:{session_id}", mapping={
        "user_id":     "demo_user_001",
        "created_at":  datetime.now().isoformat(),
        "last_active": datetime.now().isoformat()
    })
    r.expire(f"session:{session_id}", 1800)
    session = r.hgetall(f"session:{session_id}")
    ttl     = r.ttl(f"session:{session_id}")
    print(f"    Session: {session_id[:16]}...")
    print(f"    User: {session['user_id']} | TTL: {ttl}s")


def verify_consistency():
    print(f"\n{'='*60}")
    print("VERIFY: Kiểm tra tính nhất quán")
    print('='*60)
    errors = []
    result = list(db.orders.aggregate([
        {"$lookup":{"from":"customers","localField":"customer_id",
                    "foreignField":"_id","as":"c"}},
        {"$match":{"c":{"$size":0}}}, {"$count":"total"}
    ]))
    count = result[0]["total"] if result else 0
    if count > 0: errors.append(f"❌ {count} orders có customer_id không tồn tại")
    else: print("  ✅ orders.customer_id → customers: OK")

    result = list(db.reviews.aggregate([
        {"$lookup":{"from":"orders","localField":"order_id","foreignField":"_id","as":"o"}},
        {"$unwind":"$o"},
        {"$match":{"o.current_status":{"$ne":"COMPLETED"}}},
        {"$count":"total"}
    ]))
    count = result[0]["total"] if result else 0
    if count > 0: errors.append(f"❌ {count} reviews thuộc đơn chưa COMPLETED")
    else: print("  ✅ reviews chỉ từ đơn COMPLETED: OK")

    result = list(db.orders.aggregate([
        {"$addFields":{"expected":{"$subtract":[{"$add":["$items_subtotal","$delivery_fee"]},"$discount_amount"]}}},
        {"$match":{"$expr":{"$ne":["$total_amount","$expected"]}}},
        {"$count":"total"}
    ]))
    count = result[0]["total"] if result else 0
    if count > 0: errors.append(f"❌ {count} orders tính tiền sai")
    else: print("  ✅ total_amount = subtotal + fee - discount: OK")

    result = list(db.orders.aggregate([
        {"$addFields":{"last":{"$last":"$status_history.status"}}},
        {"$match":{"$expr":{"$ne":["$current_status","$last"]}}},
        {"$count":"total"}
    ]))
    count = result[0]["total"] if result else 0
    if count > 0: errors.append(f"❌ {count} orders current_status lệch status_history")
    else: print("  ✅ current_status khớp status_history[-1]: OK")

    result = list(db.reviews.aggregate([
        {"$group":{"_id":"$restaurant_id","real_avg":{"$avg":"$overall_rating"},"cnt":{"$sum":1}}},
        {"$lookup":{"from":"restaurants","localField":"_id","foreignField":"_id","as":"r"}},
        {"$unwind":"$r"},
        {"$match":{"$expr":{"$or":[
            {"$gt":[{"$abs":{"$subtract":["$real_avg","$r.avg_rating"]}},0.05]},
            {"$ne":["$cnt","$r.total_reviews"]}
        ]}}},
        {"$count":"total"}
    ]))
    count = result[0]["total"] if result else 0
    if count > 0: errors.append(f"⚠️  {count} restaurants avg_rating lệch reviews thực")
    else: print("  ✅ restaurants.avg_rating khớp reviews thực: OK")

    print(f"\n{'🎉 Tất cả checks PASSED!' if not errors else chr(10).join(errors)}")


if __name__ == "__main__":
    import uuid
    rows = list(cass.execute("SELECT customer_id FROM order_history_by_customer LIMIT 1"))
    cid_cass = str(rows[0].customer_id) if rows else str(uuid.uuid4())

    func1_get_restaurants_with_menu(district="Quan 1", category="main")
    func2_restaurant_stats_by_month(2024, 11)
    func3_customer_history_cassandra(cid_cass)
    func4_redis_realtime()
    verify_consistency()