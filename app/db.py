import os
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv
# import os
# from pymongo import MongoClient


# load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
client = AsyncIOMotorClient(MONGO_URI)
db = client["railway_db"]
collection = db["items"]

