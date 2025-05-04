from fastapi import FastAPI, HTTPException, Body, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from motor.motor_asyncio import AsyncIOMotorClient
from typing import Dict, Any, List
from bson import ObjectId
import os
import pandas as pd

# ---------- 🚀 App ----------
app = FastAPI()

# ---------- 🔓 CORS ----------
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------- ⚙️ Default MongoDB (Railway) ----------
MONGO_URI = os.getenv("MONGO_URL")
default_client = AsyncIOMotorClient(MONGO_URI)
default_db = default_client["railway_db"]
cluster_lookup_collection = default_db["cluster_connections"]

# ---------- 🔧 Utility ----------
def serialize(item) -> dict:
    item["id"] = str(item["_id"])
    del item["_id"]
    return item

def get_collection(client: AsyncIOMotorClient, db: str, collection: str):
    return client[db][collection]

# ---------- ✅ Register a Cluster ----------
@app.post("/clusters/register")
async def register_cluster(
    clustername: str = Body(...),
    mongouri: str = Body(...),
    owner: str = Body(...),
    password: str = Body(...)
):
    existing_name = await cluster_lookup_collection.find_one({"cluster": clustername})
    existing_uri = await cluster_lookup_collection.find_one({"uri": mongouri})
    if existing_name:
        raise HTTPException(status_code=400, detail=f"Cluster name '{clustername}' already exists")
    if existing_uri:
        raise HTTPException(status_code=400, detail="This MongoDB URI is already registered")

    await cluster_lookup_collection.insert_one({
        "cluster": clustername,
        "uri": mongouri,
        "owner": owner,
        "password": password
    })
    return {"status": "registered", "cluster": clustername, "owner": owner, "password": password}

# ---------- 📊 List Registered Clusters ----------
@app.post("/clusters")
async def list_clusters(
    owner: str = Body(...),
    password: str = Body(...)
):
    clusters = await cluster_lookup_collection.find({"owner": owner, "password": password}).to_list(length=100)
    return [{
        "cluster": c["cluster"],
        "uri": c["uri"]
    } for c in clusters]

# ---------- 📚 List All Databases ----------
@app.post("/databases")
async def list_databases(mongouri: str = Body(...)):
    try:
        client = AsyncIOMotorClient(mongouri)
        dbs = await client.list_database_names()
        return {"databases": dbs}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ---------- 📁 List Collections in DB ----------
@app.post("/collections")
async def list_collections(
    mongouri: str = Body(...),
    db: str = Body(...)
):
    try:
        client = AsyncIOMotorClient(mongouri)
        db_obj = client[db]
        collections = await db_obj.list_collection_names()
        return {"database": db, "collections": collections}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ---------- ✅ Insert One ----------
@app.post("/items")
async def create_item(
    mongouri: str = Body(...),
    db: str = Body(...),
    collection: str = Body(...),
    item: Dict[str, Any] = Body(...)
):
    client = AsyncIOMotorClient(mongouri)
    col = get_collection(client, db, collection)
    result = await col.insert_one(item)
    new_item = await col.find_one({"_id": result.inserted_id})
    return serialize(new_item)

# ---------- 📦 Get All Items ----------
@app.post("/items/all")
async def get_items(
    mongouri: str = Body(...),
    db: str = Body(...),
    collection: str = Body(...)
):
    client = AsyncIOMotorClient(mongouri)
    col = get_collection(client, db, collection)
    items = await col.find().to_list(100)
    return [serialize(item) for item in items]

# ---------- 🔍 Query One ----------
@app.post("/items/query")
async def query_item(
    mongouri: str = Body(...),
    db: str = Body(...),
    collection: str = Body(...),
    query: Dict[str, Any] = Body(...)
):
    client = AsyncIOMotorClient(mongouri)
    col = get_collection(client, db, collection)
    item = await col.find_one(query)
    if not item:
        raise HTTPException(status_code=404, detail="Item not found")
    return serialize(item)

# ---------- 📥 Insert Many ----------
@app.post("/items/bulk")
async def insert_many_items(
    mongouri: str = Body(...),
    db: str = Body(...),
    collection: str = Body(...),
    items: List[Dict[str, Any]] = Body(...)
):
    if not items:
        raise HTTPException(status_code=400, detail="No data to insert")

    client = AsyncIOMotorClient(mongouri)
    col = get_collection(client, db, collection)
    result = await col.insert_many(items)
    inserted = await col.find({"_id": {"$in": result.inserted_ids}}).to_list(len(result.inserted_ids))
    return [serialize(item) for item in inserted]

# ---------- 🌐 Drop & Import ----------
@app.post("/items/reset-and-import")
async def drop_and_import(
    mongouri: str = Body(...),
    db: str = Body(...),
    collection: str = Body(...),
    link: str = Body(...)
):
    client = AsyncIOMotorClient(mongouri)
    col = get_collection(client, db, collection)
    await col.drop()

    try:
        if "csv" in link:
            df = pd.read_csv(link)
        elif "xls" in link:
            df = pd.read_excel(link, engine='openpyxl')
        else:
            raise HTTPException(400, "Unsupported file type")
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to load file: {str(e)}")

    df = df.fillna("")
    items = df.to_dict(orient="records")
    if not items:
        raise HTTPException(status_code=400, detail="No data to insert")

    result = await col.insert_many(items)
    sample = await col.find().to_list(3)
    return {
        "status": "imported",
        "inserted_count": len(result.inserted_ids),
        "sample": [serialize(item) for item in sample]
    }

# ---------- ✏️ Update One ----------
@app.put("/items/{item_id}")
async def update_item(
    item_id: str,
    mongouri: str = Body(...),
    db: str = Body(...),
    collection: str = Body(...),
    update: Dict[str, Any] = Body(...)
):
    client = AsyncIOMotorClient(mongouri)
    col = get_collection(client, db, collection)
    result = await col.update_one({"_id": ObjectId(item_id)}, {"$set": update})
    if result.modified_count == 0:
        raise HTTPException(status_code=404, detail="Item not found")
    updated_item = await col.find_one({"_id": ObjectId(item_id)})
    return serialize(updated_item)

# ---------- ❌ Delete One ----------
@app.delete("/items/{item_id}")
async def delete_item(
    item_id: str,
    mongouri: str = Body(...),
    db: str = Body(...),
    collection: str = Body(...)
):
    client = AsyncIOMotorClient(mongouri)
    col = get_collection(client, db, collection)
    result = await col.delete_one({"_id": ObjectId(item_id)})
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Item not found")
    return {"status": "deleted"}
