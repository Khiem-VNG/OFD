from neo4j import GraphDatabase
from pymongo import MongoClient
from bson import ObjectId

# ============================================================
# CONNECTION
# ============================================================

driver = GraphDatabase.driver(
    "bolt://localhost:7687",
    auth=("neo4j", "password123")
)

db = MongoClient(
    "mongodb://admin:admin123@localhost:27017"
)["food_delivery"]


# ============================================================
# RECOMMEND RESTAURANTS
# ============================================================

def recommend_restaurants(customer_id):

    query = """
    MATCH (u:User {id: $customer_id})-[v:VIEWED]->(r:Restaurant)

    RETURN r.id AS restaurant_id,
           v.count AS total_views

    ORDER BY total_views DESC
    LIMIT 5
    """

    with driver.session() as session:

        result = session.run(
            query,
            customer_id=str(customer_id)
        )

        recommendations = []

        for record in result:

            restaurant = db.restaurants.find_one({
                "_id": ObjectId(record["restaurant_id"])
            })

            if restaurant:
                recommendations.append({
                    "name": restaurant["name"],
                    "views": record["total_views"]
                })

        return recommendations


# ============================================================
# RECOMMEND FOODS
# ============================================================

def recommend_foods(customer_id):

    query = """
    MATCH (u:User {id: $customer_id})-[o:ORDERED]->(f:Food)

    RETURN f.id AS food_id,
           o.count AS total_orders

    ORDER BY total_orders DESC
    LIMIT 5
    """

    with driver.session() as session:

        result = session.run(
            query,
            customer_id=str(customer_id)
        )

        foods = []

        for record in result:

            food = db.menu_items.find_one({
                "_id": ObjectId(record["food_id"])
            })

            if food:
                foods.append({
                    "food_name": food["name"],
                    "orders": record["total_orders"]
                })

        return foods


# ============================================================
# TOP RATED FOODS
# ============================================================

def top_rated_foods():

    query = """
    MATCH (:User)-[r:RATED]->(f:Food)

    RETURN f.id AS food_id,
           avg(r.rating) AS avg_rating,
           count(r) AS total_reviews

    ORDER BY avg_rating DESC, total_reviews DESC
    LIMIT 10
    """

    with driver.session() as session:

        result = session.run(query)

        foods = []

        for record in result:

            food = db.menu_items.find_one({
                "_id": ObjectId(record["food_id"])
            })

            if food:
                foods.append({
                    "food_name": food["name"],
                    "rating": round(record["avg_rating"], 2),
                    "reviews": record["total_reviews"]
                })

        return foods


# ============================================================
# DEMO
# ============================================================

if __name__ == "__main__":

    customer = db.customers.find_one()

    print("\n=== RECOMMENDED RESTAURANTS ===")
    print(recommend_restaurants(customer["_id"]))

    print("\n=== RECOMMENDED FOODS ===")
    print(recommend_foods(customer["_id"]))

    print("\n=== TOP RATED FOODS ===")
    print(top_rated_foods())