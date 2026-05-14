from neo4j import GraphDatabase
from pymongo import MongoClient

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
# CREATE GRAPH RELATIONSHIPS
# ============================================================

def log_view_restaurant(customer_id, restaurant_id):

    query = """
    MERGE (u:User {id: $customer_id})
    MERGE (r:Restaurant {id: $restaurant_id})

    MERGE (u)-[v:VIEWED]->(r)

    ON CREATE SET
        v.count = 1,
        v.last_viewed = datetime()

    ON MATCH SET
        v.count = v.count + 1,
        v.last_viewed = datetime()
    """

    with driver.session() as session:
        session.run(
            query,
            customer_id=str(customer_id),
            restaurant_id=str(restaurant_id)
        )


def log_order_food(customer_id, food_id, restaurant_id):

    query = """
    MERGE (u:User {id: $customer_id})
    MERGE (f:Food {id: $food_id})
    MERGE (r:Restaurant {id: $restaurant_id})

    MERGE (f)-[:BELONGS_TO]->(r)

    MERGE (u)-[o:ORDERED]->(f)

    ON CREATE SET
        o.count = 1,
        o.last_ordered = datetime()

    ON MATCH SET
        o.count = o.count + 1,
        o.last_ordered = datetime()
    """

    with driver.session() as session:
        session.run(
            query,
            customer_id=str(customer_id),
            food_id=str(food_id),
            restaurant_id=str(restaurant_id)
        )


def log_review(customer_id, food_id, rating):

    query = """
    MERGE (u:User {id: $customer_id})
    MERGE (f:Food {id: $food_id})

    MERGE (u)-[r:RATED]->(f)

    SET r.rating = $rating,
        r.updated_at = datetime()
    """

    with driver.session() as session:
        session.run(
            query,
            customer_id=str(customer_id),
            food_id=str(food_id),
            rating=rating
        )


# ============================================================
# DEMO TEST
# ============================================================

if __name__ == "__main__":

    customer = db.customers.find_one()
    restaurant = db.restaurants.find_one()
    food = db.menu_items.find_one()

    log_view_restaurant(
        customer["_id"],
        restaurant["_id"]
    )

    log_order_food(
        customer["_id"],
        food["_id"],
        restaurant["_id"]
    )

    log_review(
        customer["_id"],
        food["_id"],
        5
    )

    print("✅ Graph activity inserted!")