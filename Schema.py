from pymongo import MongoClient, ASCENDING, TEXT
from cassandra.cluster import Cluster
import redis
import time

def setup_mongodb():
    client = MongoClient("mongodb://admin:admin123@localhost:27017")
    db = client["food_delivery"]
    existing = db.list_collection_names()
    print("=== MongoDB Schema Setup ===")

    if "customers" not in existing:
        db.create_collection("customers", validator={"$jsonSchema": {
            "bsonType": "object",
            "required": ["full_name", "phone", "email", "addresses", "registered_at", "status"],
            "properties": {
                "full_name":     {"bsonType": "string"},
                "phone":         {"bsonType": "string"},
                "email":         {"bsonType": "string"},
                "gender":        {"bsonType": "string", "enum": ["male", "female", "other"]},
                "birth_year":    {"bsonType": "int"},
                "registered_at": {"bsonType": "date"},
                "status":        {"bsonType": "string", "enum": ["active", "blocked"]},
                "addresses": {
                    "bsonType": "array", "minItems": 1,
                    "items": {"bsonType": "object",
                              "required": ["label", "street", "district", "city", "is_default"],
                              "properties": {
                                  "label":      {"bsonType": "string"},
                                  "street":     {"bsonType": "string"},
                                  "district":   {"bsonType": "string"},
                                  "city":       {"bsonType": "string"},
                                  "lat":        {"bsonType": "double"},
                                  "lng":        {"bsonType": "double"},
                                  "is_default": {"bsonType": "bool"}
                              }}
                }
            }
        }})
    db.customers.create_index([("phone", ASCENDING)], unique=True)
    db.customers.create_index([("email", ASCENDING)], unique=True)
    db.customers.create_index([("status", ASCENDING)])
    print("✅ customers")

    if "restaurants" not in existing:
        db.create_collection("restaurants", validator={"$jsonSchema": {
            "bsonType": "object",
            "required": ["name", "phone", "street", "district", "city", "is_active"],
            "properties": {
                "name":          {"bsonType": "string"},
                "phone":         {"bsonType": "string"},
                "street":        {"bsonType": "string"},
                "district":      {"bsonType": "string"},
                "city":          {"bsonType": "string"},
                "is_active":     {"bsonType": "bool"},
                "avg_rating":    {"bsonType": "double", "minimum": 0, "maximum": 5},
                "total_reviews": {"bsonType": "int"}
            }
        }})
    db.restaurants.create_index([("district", ASCENDING), ("avg_rating", ASCENDING)])
    db.restaurants.create_index([("is_active", ASCENDING)])
    print("✅ restaurants")

    if "menu_items" not in existing:
        db.create_collection("menu_items", validator={"$jsonSchema": {
            "bsonType": "object",
            "required": ["restaurant_id", "name", "category", "price", "is_available"],
            "properties": {
                "restaurant_id": {"bsonType": "objectId"},
                "name":          {"bsonType": "string"},
                "category":      {"bsonType": "string", "enum": ["main", "drink", "dessert", "snack"]},
                "price":         {"bsonType": "int", "minimum": 0},
                "is_available":  {"bsonType": "bool"}
            }
        }})
    db.menu_items.create_index([("restaurant_id", ASCENDING), ("category", ASCENDING)])
    db.menu_items.create_index([("is_available", ASCENDING)])
    db.menu_items.create_index([("name", TEXT), ("description", TEXT)])
    print("✅ menu_items")

    if "orders" not in existing:
        db.create_collection("orders", validator={"$jsonSchema": {
            "bsonType": "object",
            "required": ["customer_id", "restaurant_id", "items", "status_history",
                         "current_status", "payment_method", "total_amount", "created_at"],
            "properties": {
                "customer_id":    {"bsonType": "objectId"},
                "restaurant_id":  {"bsonType": "objectId"},
                "current_status": {"bsonType": "string",
                                   "enum": ["PLACED","CONFIRMED","PREPARING",
                                            "PICKED_UP","DELIVERING","COMPLETED","CANCELLED"]},
                "payment_method": {"bsonType": "string",
                                   "enum": ["CASH","MOMO","ZALOPAY","CARD"]},
                "items":          {"bsonType": "array", "minItems": 1},
                "total_amount":   {"bsonType": "int", "minimum": 0}
            }
        }})
    db.orders.create_index([("customer_id", ASCENDING), ("created_at", ASCENDING)])
    db.orders.create_index([("restaurant_id", ASCENDING), ("current_status", ASCENDING)])
    db.orders.create_index([("created_at", ASCENDING)])
    print("✅ orders")

    if "reviews" not in existing:
        db.create_collection("reviews", validator={"$jsonSchema": {
            "bsonType": "object",
            "required": ["order_id", "customer_id", "restaurant_id", "overall_rating", "created_at"],
            "properties": {
                "overall_rating":          {"bsonType": "int", "minimum": 1, "maximum": 5},
                "food_quality_rating":     {"bsonType": "int", "minimum": 1, "maximum": 5},
                "delivery_speed_rating":   {"bsonType": "int", "minimum": 1, "maximum": 5}
            }
        }})
    db.reviews.create_index([("order_id", ASCENDING)], unique=True)
    db.reviews.create_index([("restaurant_id", ASCENDING), ("created_at", ASCENDING)])
    print("✅ reviews")

    if "restaurant_daily_stats" not in existing:
        db.create_collection("restaurant_daily_stats")
    db.restaurant_daily_stats.create_index(
        [("restaurant_id", ASCENDING), ("date", ASCENDING)], unique=True)
    print("✅ restaurant_daily_stats")

    if "customer_activity" not in existing:
        db.create_collection("customer_activity")
    db.customer_activity.create_index([("customer_id", ASCENDING), ("timestamp", ASCENDING)])
    print("✅ customer_activity")

    print("✅ MongoDB setup hoàn tất!\n")
    client.close()


def setup_cassandra():
    print("=== Cassandra Schema Setup ===")
    cluster = Cluster(["localhost"], port=9042)
    session = cluster.connect()

    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS food_delivery
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
    """)
    session.set_keyspace("food_delivery")

    session.execute("""
        CREATE TABLE IF NOT EXISTS customer_activity (
            customer_id    UUID,
            timestamp      TIMESTAMP,
            event_id       UUID,
            event_type     TEXT,
            restaurant_id  UUID,
            menu_item_id   UUID,
            search_keyword TEXT,
            PRIMARY KEY (customer_id, timestamp, event_id)
        ) WITH CLUSTERING ORDER BY (timestamp DESC, event_id ASC)
          AND default_time_to_live = 7776000
    """)
    print("✅ customer_activity table")

    session.execute("""
        CREATE TABLE IF NOT EXISTS order_history_by_customer (
            customer_id     UUID,
            created_at      TIMESTAMP,
            order_id        UUID,
            restaurant_name TEXT,
            current_status  TEXT,
            total_amount    DECIMAL,
            payment_method  TEXT,
            item_summary    TEXT,
            PRIMARY KEY (customer_id, created_at, order_id)
        ) WITH CLUSTERING ORDER BY (created_at DESC, order_id ASC)
    """)
    print("✅ order_history_by_customer table")

    print("✅ Cassandra setup hoàn tất!\n")
    cluster.shutdown()


def setup_redis():
    print("=== Redis Connection Check ===")
    r = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)
    r.ping()
    print("✅ Redis kết nối thành công!")
    print("✅ Redis không cần tạo schema — keys tự tạo khi sử dụng\n")


if __name__ == "__main__":
    setup_mongodb()
    setup_cassandra()
    setup_redis()
    print("🎉 Tất cả schema đã được tạo!")