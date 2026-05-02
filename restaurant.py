from pymongo import MongoClient
from cassandra.cluster import Cluster
import redis
import json
from bson import ObjectId
from datetime import datetime

db   = MongoClient("mongodb://admin:admin123@localhost:27017")["food_delivery"]
cass = Cluster(["localhost"], port=9042).connect("food_delivery")
r    = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)

def login_restaurant(username, password):
    restaurant = db.restaurants.find_one({"username": username})
    if restaurant and restaurant["password"] == password:
        print(f"✅ Restaurant '{restaurant['name']}' logged in successfully!")
        return restaurant
    else:
        print("❌ Invalid username or password for restaurant.")
        return None

def update_menu(restaurant_id, menu_items):
    result = db.restaurants.update_one(
        {"_id": ObjectId(restaurant_id)},
        {"$set": {"menu": menu_items}}
    )
    if result.modified_count > 0:
        print(f"✅ Menu updated for restaurant ID: {restaurant_id}")
    else:
        print(f"❌ Failed to update menu for restaurant ID: {restaurant_id}")


def view_orders(restaurant_id):
    orders = db.orders.find({"restaurant_id": ObjectId(restaurant_id)})
    
    print(f"\n📋 Orders for restaurant ID: {restaurant_id}")
    print("=" * 50) # Creates a double-line border
    
    for order in orders:
        print(f"Order ID:    {order['_id']}")
        print(f"Customer ID: {order['customer_id']}")
        print(f"Items:       {order['items']}")
        print(f"Total:       {order['total_amount']}")
        print(f"Status:      {order['current_status']}")
        print("-" * 50) 
        

    
def update_order_status(order_id, new_status):
    result = db.orders.update_one(
        {"_id": ObjectId(order_id)},
        {"$set": {"current_status": new_status}}
    )
    if result.modified_count > 0:
        print(f"✅ Order ID: {order_id} status updated to '{new_status}'")
    else:
        print(f"❌ Failed to update status for Order ID: {order_id}")


def view_order_by_status(restaurant_id, status):
    orders = db.orders.find({"restaurant_id": ObjectId(restaurant_id), "current_status": status})
    print(f"📋 Orders with status '{status}' for restaurant ID: {restaurant_id}")
    for order in orders:
        print(f"- Order ID: {order['_id']}, Customer ID: {order['customer_id']}, Total: {order['total_amount']}")

def get_total_income(restaurant_id):
    pipeline = [
        {"$match": {"restaurant_id": ObjectId(restaurant_id), "current_status": "COMPLETED"}},
        {"$group": {"_id": None, "total_income": {"$sum": "$total_amount"}}}
    ]
    result = list(db.orders.aggregate(pipeline))
    total_income = result[0]["total_income"] if result else 0
    print(f"💰 Total income for restaurant ID: {restaurant_id} is ${total_income:.2f}")
    return total_income


def get_reviews(restaurant_id):
    reviews = db.reviews.find({"restaurant_id": ObjectId(restaurant_id)})
    print(f"📋 Reviews for restaurant ID: {restaurant_id}")
    for review in reviews:
        print(f"- Customer ID: {review['customer_id']}")
        print(f" Overall Rating: {review['overall_rating']}/5")
        print(f" Food Quality: {review['food_quality_rating']}/5")
        print(f" Delivery Time: {review['delivery_speed_rating']}/5")
        print(f" Comment: {review['comment']}")

if __name__ == "__main__":
    # function testing
    restaurant_id = '69f35fae4390d582454e6f72'

    #view_orders(restaurant_id)
    #view_order_by_status(restaurant_id, "preparing")

    #get_total_income(restaurant_id)

    get_reviews(restaurant_id)

    
    
