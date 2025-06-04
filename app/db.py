import os
from motor.motor_asyncio import AsyncIOMotorClient
# from dotenv import load_dotenv

MONGO_URI = os.getenv("MY_MONGO")
print("🌐 MONGO_URI =", MONGO_URI)
client = AsyncIOMotorClient(MONGO_URI)
db = client["railway_db"]
collection = db["items"]

