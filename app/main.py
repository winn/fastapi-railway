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

async def get_client_from_cluster(cluster: str = Query("default")) -> AsyncIOMotorClient:
    if not cluster or cluster == "default":
        return default_client

    cluster_doc = await cluster_lookup_collection.find_one({"cluster": cluster})
    if not cluster_doc:
        raise HTTPException(status_code=404, detail=f"Cluster '{cluster}' not found")
    uri = cluster_doc["uri"]
    return AsyncIOMotorClient(uri)

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
@app.get("/databases")
async def list_databases(cluster: str = Query("default")):
    client = await get_client_from_cluster(cluster)
    try:
        dbs = await client.list_database_names()
        return {"databases": dbs}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ---------- 📁 List Collections in DB ----------
@app.get("/collections")
async def list_collections(db: str = Query(...), cluster: str = Query("default")):
    client = await get_client_from_cluster(cluster)
    try:
        db_obj = client[db]
        collections = await db_obj.list_collection_names()
        return {"database": db, "collections": collections}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Remaining endpoints stay unchanged ...
