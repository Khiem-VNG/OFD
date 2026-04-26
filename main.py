from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()
mongo_uri = os.getenv("MONGO_URI")

client = MongoClient(mongo_uri)

db = client["db_name"]



#Sign up new customer

#Sign up new restaurant

#Log in as customer

#log in as restaurant

#retrieve restaurant information

#add new dish (restaurant only)

#place order (customer only) - Create new order document with status "pending"

#confirm order (restaurant only) - update order status to "confirmed"

#update order status (restaurant only) - update status to shipping / delivered / cancelled

#view order status (customer only) - retrieve current order status

#pay for order (?? customer / restaurant)

#view order history (customer - restaurant)

#search for dishes (customer only)

#search for restaurants (customer only optional)

#Restaurant income tracking (restaurant only)

#Post reviews (customer only) - create review for restaurant (required paid & delivered/ cancelled orders)

