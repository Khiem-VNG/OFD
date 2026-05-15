"""
graph_benchmark.py
==================
Benchmark 4 loại query graph dùng trong customer.py + user_activity_graph.py

Schema Neo4j thực tế (từ user_activity_graph.py):
  (:User)-[:VIEWED   {count, last_viewed}  ]->(:Restaurant)
  (:User)-[:ORDERED  {count, last_ordered} ]->(:Food)
  (:User)-[:RATED    {rating, updated_at}  ]->(:Food)
  (:Food)-[:BELONGS_TO                     ]->(:Restaurant)

  Q1 — Food Recommendation      User→ORDERED→Food←ORDERED←SimilarUser  (3-hop)
  Q2 — Restaurant Popularity    Food←RATED←User + Food→BELONGS_TO→Rest (2-hop aggregation)
  Q3 — Customer Activity Path   User→[VIEWED|ORDERED|RATED]  (fan-out)
  Q4 — Collaborative Filtering  User→Food←User→Food→Restaurant          (4-hop)

Cách chạy:
    python graph_benchmark.py
    python graph_benchmark.py --runs 10
    python graph_benchmark.py --customer-index 2
"""

import time
import uuid
import argparse
import statistics
from bson import ObjectId
from pymongo import MongoClient
from cassandra.cluster import Cluster
from neo4j import GraphDatabase

# ============================================================
# KẾT NỐI
# ============================================================


mongo_db = MongoClient(
    "mongodb://admin:admin123@127.0.0.1:27017/?authSource=admin"
)["food_delivery"]

cass = Cluster(["localhost"], port=9042).connect("food_delivery")

neo4j_driver = GraphDatabase.driver(
    "bolt://localhost:7687",
    auth=("neo4j", "password123")
)

# ============================================================
# HELPER
# ============================================================

BOLD  = "\033[1m"
RESET = "\033[0m"
GREEN = "\033[92m"
CYAN  = "\033[96m"
YELLOW= "\033[93m"
RED   = "\033[91m"
DIM   = "\033[2m"

def header(title):
    print(f"\n{'='*62}")
    print(f"  {BOLD}{title}{RESET}")
    print(f"{'='*62}")

def subheader(db_name, color=CYAN):
    print(f"\n  {color}▶ {db_name}{RESET}")

def measure(fn, runs=5):
    """Chạy fn nhiều lần, trả về (mean, min, max, stddev) tính bằng ms"""
    times = []
    for _ in range(runs):
        t0 = time.perf_counter()
        result = fn()
        times.append((time.perf_counter() - t0) * 1000)
    return {
        "mean":   statistics.mean(times),
        "min":    min(times),
        "max":    max(times),
        "stddev": statistics.stdev(times) if len(times) > 1 else 0,
        "result": result,
    }

def print_stat(label, stat, is_winner=False):
    tag = f"  {GREEN}★ WINNER{RESET}" if is_winner else ""
    print(f"    {label:<12}  mean={stat['mean']:7.3f}ms  "
          f"min={stat['min']:7.3f}ms  max={stat['max']:7.3f}ms  "
          f"σ={stat['stddev']:5.3f}ms{tag}")

def winner_label(stats: dict):
    """Trả về key có mean thấp nhất"""
    return min(stats, key=lambda k: stats[k]["mean"])

def to_cassandra_uuid(mongo_oid):
    return uuid.uuid5(uuid.NAMESPACE_OID, str(mongo_oid))

# ============================================================
# Q1 — FOOD RECOMMENDATION
# "Gợi ý món ăn dựa trên người dùng có hành vi tương tự"
# Neo4j: 3-hop  User→ORDERED→Food←ORDERED←SimilarUser
# MongoDB: $lookup pipeline (joins thủ công)
# Cassandra: KHÔNG hỗ trợ join — giả lập bằng 2 queries riêng
# ============================================================

def q1_neo4j(customer_id_str, runs):
    def fn():
        query = """
        MATCH (u:User {id: $uid})-[:ORDERED]->(f:Food)<-[:ORDERED]-(other:User)
        WHERE other.id <> $uid
        WITH f, count(other) AS popularity
        ORDER BY popularity DESC
        LIMIT 10
        RETURN f.id AS food_id, f.name AS food_name, popularity
        """
        with neo4j_driver.session() as session:
            return list(session.run(query, uid=customer_id_str))
    return measure(fn, runs)


def q1_mongo(customer_oid, runs):
    def fn():
        # Bước 1: lấy các food mà customer đã order
        my_orders = list(mongo_db.orders.find(
            {"customer_id": customer_oid},
            {"items.menu_item_id": 1}
        ))
        my_item_ids = {
            item["menu_item_id"]
            for o in my_orders
            for item in o.get("items", [])
        }
        if not my_item_ids:
            return []

        # Bước 2: tìm customer khác cũng order các món đó
        similar_orders = list(mongo_db.orders.find(
            {"items.menu_item_id": {"$in": list(my_item_ids)},
             "customer_id": {"$ne": customer_oid}},
            {"customer_id": 1}
        ))
        similar_customers = list({o["customer_id"] for o in similar_orders})
        if not similar_customers:
            return []

        # Bước 3: lấy món họ thích (popularity)
        pipeline = [
            {"$match": {"customer_id": {"$in": similar_customers}}},
            {"$unwind": "$items"},
            {"$match": {"items.menu_item_id": {"$nin": list(my_item_ids)}}},
            {"$group": {
                "_id":        "$items.menu_item_id",
                "name":       {"$first": "$items.name"},
                "popularity": {"$sum": 1}
            }},
            {"$sort":  {"popularity": -1}},
            {"$limit": 10},
        ]
        return list(mongo_db.orders.aggregate(pipeline))
    return measure(fn, runs)


def q1_cassandra(customer_oid, runs):
    """
    Cassandra không có JOIN. Giả lập bằng:
    1. Lấy order_history của customer → danh sách item
    2. Lấy item phổ biến nhất toàn bộ hệ thống (không thể join like-minded users)
    → Đây là giới hạn của Cassandra cho bài toán này
    """
    cid = to_cassandra_uuid(customer_oid)

    def fn():
        # Lấy lịch sử đơn hàng của customer
        rows = list(cass.execute("""
            SELECT item_summary FROM order_history_by_customer
            WHERE customer_id = %s LIMIT 20
        """, (cid,)))

        # Lấy top items từ toàn bộ activity (best effort, không có join)
        activities = list(cass.execute("""
            SELECT menu_item_id, count(*) as cnt
            FROM customer_activity
            WHERE event_type = 'ORDER_PLACED'
            LIMIT 100
            ALLOW FILTERING
        """))
        return activities[:10]
    return measure(fn, runs)


# ============================================================
# Q2 — RESTAURANT POPULARITY SCORE
# "Tính điểm nổi bật của nhà hàng từ review + order graph"
# Neo4j: traverse User→REVIEWED→Restaurant + User→ORDERED→Food→Restaurant
# MongoDB: $lookup + $group aggregation
# Cassandra: scan + aggregate thủ công (limited)
# ============================================================

def q2_neo4j(restaurant_id_str, runs):
    """
    Schema thực tế: KHÔNG có User→REVIEWED→Restaurant
    Thay bằng:  User-[:RATED]->Food-[:BELONGS_TO]->Restaurant
                User-[:ORDERED]->Food-[:BELONGS_TO]->Restaurant
    """
    def fn():
        query = """
        MATCH (r:Restaurant {id: $rid})<-[:BELONGS_TO]-(f:Food)
        OPTIONAL MATCH (u:User)-[rt:RATED]->(f)
        WITH r, count(rt) AS review_count, avg(rt.rating) AS avg_rating
        OPTIONAL MATCH (u2:User)-[o:ORDERED]->(f2:Food)-[:BELONGS_TO]->(r)
        WITH r, review_count, avg_rating,
             sum(coalesce(o.count, 1)) AS order_count
        RETURN r.id          AS restaurant_id,
               review_count  AS total_reviews,
               avg_rating    AS avg_review,
               order_count   AS total_orders,
               (coalesce(avg_rating, 0) * 0.6 + log(order_count + 1) * 0.4)
                             AS popularity_score
        """
        with neo4j_driver.session() as session:
            return list(session.run(query, rid=restaurant_id_str))
    return measure(fn, runs)


def q2_mongo(restaurant_oid, runs):
    def fn():
        # Reviews stats
        review_agg = list(mongo_db.reviews.aggregate([
            {"$match": {"restaurant_id": restaurant_oid}},
            {"$group": {
                "_id":        None,
                "avg_rating": {"$avg": "$overall_rating"},
                "total":      {"$sum": 1}
            }}
        ]))

        # Order stats — đếm orders chứa món của nhà hàng
        order_count = mongo_db.orders.count_documents(
            {"restaurant_id": restaurant_oid}
        )

        avg_r = review_agg[0]["avg_rating"] if review_agg else 0
        total = review_agg[0]["total"]       if review_agg else 0
        import math
        score = avg_r * 0.6 + math.log(order_count + 1) * 0.4
        return {"avg_rating": avg_r, "total_reviews": total,
                "order_count": order_count, "popularity_score": score}
    return measure(fn, runs)


def q2_cassandra(customer_oid, runs):
    """
    Cassandra không partition by restaurant thẳng được — ta đọc
    order_history và filter. Đây là worst case cho Cassandra.
    """
    cid = to_cassandra_uuid(customer_oid)

    def fn():
        rows = list(cass.execute("""
            SELECT restaurant_name, current_status
            FROM order_history_by_customer
            WHERE customer_id = %s LIMIT 50
        """, (cid,)))

        completed = [r for r in rows if r.current_status == "COMPLETED"]
        return {"completed_orders": len(completed), "total": len(rows)}
    return measure(fn, runs)


# ============================================================
# Q3 — CUSTOMER ACTIVITY PATH
# "Lấy toàn bộ hành vi của customer: VIEWED, ORDERED, RATED"
# Neo4j: fan-out từ 1 User node qua 3 relationship types
# Cassandra: native time-series (HOME TURF — partition by customer_id)
# MongoDB: find + sort (document lookup trên orders collection)
# ============================================================

def q3_neo4j(customer_id_str, runs):
    """
    Schema thực tế:
      User-[:VIEWED  {count, last_viewed}  ]->Restaurant
      User-[:ORDERED {count, last_ordered} ]->Food
      User-[:RATED   {rating, updated_at}  ]->Food
    Không có timestamp per-event → sort by last_viewed / last_ordered
    """
    def fn():
        query = """
        MATCH (u:User {id: $uid})
        OPTIONAL MATCH (u)-[v:VIEWED]->(r:Restaurant)
        OPTIONAL MATCH (u)-[o:ORDERED]->(f:Food)
        OPTIONAL MATCH (u)-[rt:RATED]->(f2:Food)
        RETURN
            count(DISTINCT r)  AS restaurants_viewed,
            count(DISTINCT f)  AS foods_ordered,
            count(DISTINCT f2) AS foods_rated,
            sum(v.count)       AS total_views,
            sum(o.count)       AS total_orders
        """
        with neo4j_driver.session() as session:
            return list(session.run(query, uid=customer_id_str))
    return measure(fn, runs)


def q3_cassandra(customer_oid, runs):
    """Cassandra: native time-series, partition by customer_id — đây là HOME TURF"""
    cid = to_cassandra_uuid(customer_oid)

    def fn():
        return list(cass.execute("""
            SELECT timestamp, event_type, restaurant_id, menu_item_id
            FROM customer_activity
            WHERE customer_id = %s
            ORDER BY timestamp DESC
            LIMIT 20
        """, (cid,)))
    return measure(fn, runs)


def q3_mongo(customer_oid, runs):
    def fn():
        # Lấy orders làm proxy cho activity
        return list(mongo_db.orders.find(
            {"customer_id": customer_oid},
            {"current_status": 1, "created_at": 1, "restaurant_id": 1, "items": 1}
        ).sort("created_at", -1).limit(20))
    return measure(fn, runs)


# ============================================================
# Q4 — COLLABORATIVE FILTERING (Neo4j ONLY showcase)
# "Tìm customer tương tự và gợi ý nhà hàng chưa thử"
# MongoDB: multi-stage $lookup — expensive
# Cassandra: không thể — không có JOIN
# ============================================================

def q4_neo4j(customer_id_str, runs):
    def fn():
        query = """
        MATCH (u:User {id: $uid})-[:ORDERED]->(f:Food)<-[:ORDERED]-(similar:User)
        WHERE similar.id <> $uid
        WITH similar, count(f) AS shared_foods
        ORDER BY shared_foods DESC
        LIMIT 5
        MATCH (similar)-[:ORDERED]->(:Food)-[:BELONGS_TO]->(r:Restaurant)
        WHERE NOT EXISTS {
            MATCH (u)-[:ORDERED]->(:Food)-[:BELONGS_TO]->(r)
        }
        RETURN r.id AS restaurant_id, r.name AS restaurant_name,
               count(DISTINCT similar) AS recommenders
        ORDER BY recommenders DESC
        LIMIT 10
        """
        with neo4j_driver.session() as session:
            return list(session.run(query, uid=customer_id_str))
    return measure(fn, runs)


def q4_mongo(customer_oid, runs):
    """MongoDB: giả lập collaborative filter — 3 stage aggregation"""
    def fn():
        # Stage 1: food customer đã order
        my_items = {
            item["menu_item_id"]
            for o in mongo_db.orders.find({"customer_id": customer_oid}, {"items.menu_item_id": 1})
            for item in o.get("items", [])
        }
        if not my_items:
            return []

        # Stage 2: similar customers (cùng order các món đó)
        similar = list({
            o["customer_id"]
            for o in mongo_db.orders.find(
                {"items.menu_item_id": {"$in": list(my_items)},
                 "customer_id": {"$ne": customer_oid}},
                {"customer_id": 1}
            )
        })[:5]

        if not similar:
            return []

        # Stage 3: restaurants similar customers đã order, mà customer chưa thử
        my_restaurants = {
            o["restaurant_id"]
            for o in mongo_db.orders.find({"customer_id": customer_oid}, {"restaurant_id": 1})
        }

        pipeline = [
            {"$match": {
                "customer_id":  {"$in": similar},
                "restaurant_id": {"$nin": list(my_restaurants)}
            }},
            {"$group": {
                "_id":         "$restaurant_id",
                "recommenders": {"$addToSet": "$customer_id"}
            }},
            {"$project": {
                "restaurant_id": "$_id",
                "recommender_count": {"$size": "$recommenders"}
            }},
            {"$sort":  {"recommender_count": -1}},
            {"$limit": 10},
        ]
        return list(mongo_db.orders.aggregate(pipeline))
    return measure(fn, runs)


def q4_cassandra_stub(runs):
    """Cassandra không hỗ trợ JOIN — không thể thực hiện collaborative filtering"""
    def fn():
        return "N/A — Cassandra không hỗ trợ cross-partition JOIN"
    return measure(fn, runs)


# ============================================================
# Q5 — TOP RATED FOODS PER RESTAURANT  (dùng RATED relationship)
# "Món ăn được đánh giá cao nhất của nhà hàng, tính từ graph"
# Neo4j: Restaurant←BELONGS_TO←Food←RATED←User  (2-hop, native)
# MongoDB: $lookup menu_items + reviews + $group
# Cassandra: N/A — không có cross-collection aggregation
# ============================================================

def q5_neo4j(restaurant_id_str, runs):
    """
    Đây là query tận dụng đúng RATED relationship trong graph.
    Không cần join collection nào — graph tự nhiên chứa quan hệ này.
    """
    def fn():
        query = """
        MATCH (r:Restaurant {id: $rid})<-[:BELONGS_TO]-(f:Food)<-[rt:RATED]-(u:User)
        WITH f, avg(rt.rating) AS avg_rating, count(rt) AS rating_count
        WHERE rating_count >= 1
        ORDER BY avg_rating DESC, rating_count DESC
        LIMIT 10
        RETURN f.id        AS food_id,
               f.name      AS food_name,
               avg_rating  AS avg_rating,
               rating_count AS num_raters
        """
        with neo4j_driver.session() as session:
            return list(session.run(query, rid=restaurant_id_str))
    return measure(fn, runs)


def q5_mongo(restaurant_oid, runs):
    """MongoDB: phải join reviews (item_ratings) + menu_items"""
    def fn():
        pipeline = [
            # Lấy reviews của nhà hàng
            {"$match": {"restaurant_id": restaurant_oid}},
            {"$unwind": "$item_ratings"},
            {"$group": {
                "_id":        "$item_ratings.menu_item_id",
                "name":       {"$first": "$item_ratings.name"},
                "avg_rating": {"$avg": "$item_ratings.rating"},
                "num_raters": {"$sum": 1},
            }},
            {"$sort":  {"avg_rating": -1, "num_raters": -1}},
            {"$limit": 10},
        ]
        return list(mongo_db.reviews.aggregate(pipeline))
    return measure(fn, runs)


def q5_cassandra_stub(runs):
    """Cassandra không có cross-partition aggregation cho bài toán này"""
    def fn():
        return "N/A — không thể aggregate ratings across partitions"
    return measure(fn, runs)


# ============================================================
# MAIN BENCHMARK RUNNER
# ============================================================

def run_benchmarks(runs=5, customer_index=0):
    # Lấy customer và restaurant mẫu
    customer  = list(mongo_db.customers.find({"status": "active"}).limit(5))[customer_index]
    restaurant = mongo_db.restaurants.find_one({"is_active": True})

    cust_oid    = customer["_id"]
    cust_str    = str(cust_oid)
    rest_oid    = restaurant["_id"]
    rest_str    = str(rest_oid)

    print(f"\n{BOLD}{'='*62}{RESET}")
    print(f"{BOLD}  GRAPH BENCHMARK — customer.py use cases{RESET}")
    print(f"{'='*62}")
    print(f"  Khách hàng : {customer['full_name']} ({cust_str[-8:]}...)")
    print(f"  Nhà hàng   : {restaurant['name']}   ({rest_str[-8:]}...)")
    print(f"  Số lần chạy: {runs} lần mỗi query")
    print(f"  Đơn vị     : milliseconds (ms)")

    results = {}

    # ----------------------------------------------------------
    # Q1
    # ----------------------------------------------------------
    header("Q1 · Food Recommendation  (3-hop graph traversal)")
    print(f"  {DIM}Use case: gợi ý món ăn từ người dùng hành vi tương tự{RESET}")
    print(f"  {DIM}Neo4j: User→ORDERED→Food←ORDERED←SimilarUser{RESET}\n")

    r_neo4j = q1_neo4j(cust_str, runs)
    r_mongo  = q1_mongo(cust_oid, runs)
    r_cass   = q1_cassandra(cust_oid, runs)

    q1_stats = {"Neo4j": r_neo4j, "MongoDB": r_mongo, "Cassandra": r_cass}
    w = winner_label(q1_stats)
    results["Q1"] = q1_stats

    for db_name, stat in q1_stats.items():
        print_stat(db_name, stat, is_winner=(db_name == w))

    # ----------------------------------------------------------
    # Q2
    # ----------------------------------------------------------
    header("Q2 · Restaurant Popularity Score  (aggregation)")
    print(f"  {DIM}Use case: tính điểm nổi bật từ review + order graph{RESET}\n")

    r2_neo4j = q2_neo4j(rest_str, runs)
    r2_mongo  = q2_mongo(rest_oid, runs)
    r2_cass   = q2_cassandra(cust_oid, runs)

    q2_stats = {"Neo4j": r2_neo4j, "MongoDB": r2_mongo, "Cassandra": r2_cass}
    w2 = winner_label(q2_stats)
    results["Q2"] = q2_stats

    for db_name, stat in q2_stats.items():
        print_stat(db_name, stat, is_winner=(db_name == w2))

    # ----------------------------------------------------------
    # Q3
    # ----------------------------------------------------------
    header("Q3 · Customer Activity Profile  (fan-out graph vs time-series)")
    print(f"  {DIM}Use case: tổng hợp hành vi VIEWED / ORDERED / RATED của 1 customer{RESET}")
    print(f"  {DIM}Neo4j: 3 relationship types từ 1 User node{RESET}\n")

    r3_neo4j = q3_neo4j(cust_str, runs)
    r3_cass   = q3_cassandra(cust_oid, runs)
    r3_mongo  = q3_mongo(cust_oid, runs)

    q3_stats = {"Neo4j": r3_neo4j, "Cassandra": r3_cass, "MongoDB": r3_mongo}
    w3 = winner_label(q3_stats)
    results["Q3"] = q3_stats

    for db_name, stat in q3_stats.items():
        print_stat(db_name, stat, is_winner=(db_name == w3))

    # ----------------------------------------------------------
    # Q4
    # ----------------------------------------------------------
    header("Q4 · Collaborative Filtering  (4-hop)")
    print(f"  {DIM}Use case: 'Nhà hàng bạn chưa thử, nhưng người giống bạn thích'{RESET}")
    print(f"  {DIM}Neo4j: User→ORDERED→Food←ORDERED←SimilarUser→Food→BELONGS_TO→Restaurant{RESET}\n")

    r4_neo4j = q4_neo4j(cust_str, runs)
    r4_mongo  = q4_mongo(cust_oid, runs)
    r4_cass   = q4_cassandra_stub(runs)

    q4_stats = {"Neo4j": r4_neo4j, "MongoDB": r4_mongo, "Cassandra": r4_cass}
    w4 = winner_label({"Neo4j": r4_neo4j, "MongoDB": r4_mongo})
    results["Q4"] = q4_stats

    print_stat("Neo4j",   r4_neo4j, is_winner=(w4 == "Neo4j"))
    print_stat("MongoDB", r4_mongo, is_winner=(w4 == "MongoDB"))
    print(f"    {'Cassandra':<12}  N/A — cross-partition JOIN không được hỗ trợ")

    # ----------------------------------------------------------
    # Q5
    # ----------------------------------------------------------
    header("Q5 · Top Rated Foods per Restaurant  (RATED relationship)")
    print(f"  {DIM}Use case: món ngon nhất nhà hàng tính từ graph rating{RESET}")
    print(f"  {DIM}Neo4j: Restaurant←BELONGS_TO←Food←RATED←User  (2-hop native){RESET}\n")

    r5_neo4j = q5_neo4j(rest_str, runs)
    r5_mongo  = q5_mongo(rest_oid, runs)
    r5_cass   = q5_cassandra_stub(runs)

    q5_stats = {"Neo4j": r5_neo4j, "MongoDB": r5_mongo, "Cassandra": r5_cass}
    w5 = winner_label({"Neo4j": r5_neo4j, "MongoDB": r5_mongo})
    results["Q5"] = q5_stats

    print_stat("Neo4j",   r5_neo4j, is_winner=(w5 == "Neo4j"))
    print_stat("MongoDB", r5_mongo, is_winner=(w5 == "MongoDB"))
    print(f"    {'Cassandra':<12}  N/A — cross-partition aggregation không được hỗ trợ")

    # ----------------------------------------------------------
    # SUMMARY TABLE
    # ----------------------------------------------------------
    header("TỔNG KẾT")
    print(f"  {'Query':<10} {'Neo4j':>10} {'MongoDB':>10} {'Cassandra':>12}  {'Winner':<12}")
    print(f"  {'-'*10} {'-'*10} {'-'*10} {'-'*12}  {'-'*12}")

    for qname, stats in results.items():
        neo = f"{stats['Neo4j']['mean']:7.2f}ms"
        mdb = f"{stats['MongoDB']['mean']:7.2f}ms"

        # Q4 và Q5 Cassandra là stub (mean rất nhỏ, không tính vào winner)
        cas_is_stub = qname in ("Q4", "Q5")
        cas = "    N/A     " if cas_is_stub else f"{stats['Cassandra']['mean']:7.2f}ms"

        cmp = {"Neo4j": stats["Neo4j"], "MongoDB": stats["MongoDB"]}
        if not cas_is_stub:
            cmp["Cassandra"] = stats["Cassandra"]
        w   = winner_label(cmp)
        wc  = GREEN if w == "Neo4j" else (YELLOW if w == "MongoDB" else CYAN)
        print(f"  {qname:<10} {neo:>10} {mdb:>10} {cas:>12}  {wc}{w:<12}{RESET}")

    print()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Graph benchmark: Neo4j vs MongoDB vs Cassandra")
    parser.add_argument("--runs",           type=int, default=5,  help="Số lần chạy mỗi query (default: 5)")
    parser.add_argument("--customer-index", type=int, default=0,  help="Index của customer trong DB (default: 0)")
    args = parser.parse_args()

    run_benchmarks(runs=args.runs, customer_index=args.customer_index)