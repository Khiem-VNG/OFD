import time
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
# BENCHMARK NEO4J READ
# ============================================================

def benchmark_neo4j_read(customer_id):

    query = """
    MATCH (u:User {id: $customer_id})-[o:ORDERED]->(f:Food)
    RETURN f.id, o.count
    """

    start = time.perf_counter()

    with driver.session() as session:
        list(session.run(
            query,
            customer_id=str(customer_id)
        ))

    end = time.perf_counter()

    return end - start


# ============================================================
# BENCHMARK MONGODB READ
# ============================================================

def benchmark_mongodb_read(customer_id):

    start = time.perf_counter()

    list(db.orders.find({
        "customer_id": customer_id
    }))

    end = time.perf_counter()

    return end - start


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":

    customer = db.customers.find_one()

    neo4j_time = benchmark_neo4j_read(customer["_id"])
    mongo_time = benchmark_mongodb_read(customer["_id"])

    print("\n========== READ BENCHMARK ==========")
    print(f"Neo4j Read Time  : {neo4j_time:.6f} sec")
    print(f"MongoDB Read Time: {mongo_time:.6f} sec")

    if neo4j_time < mongo_time:
        print("\n✅ Neo4j faster for relationship traversal")
    else:
        print("\n✅ MongoDB faster for this query")