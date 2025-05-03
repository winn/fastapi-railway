# import os
# from motor.motor_asyncio import AsyncIOMotorClient
# from dotenv import load_dotenv
import os
from pymongo import MongoClient


# load_dotenv()

mongo_uri = os.getenv("MONGO_URL")
client = MongoClient(mongo_uri)
db = client["railway_db"]
collection = db["items"]

