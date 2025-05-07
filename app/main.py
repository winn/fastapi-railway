from fastapi import FastAPI, HTTPException, Request, Body, Query
from fastapi.middleware.cors import CORSMiddleware
from app.models import Item, UpdateItem
from bson import ObjectId
from typing import Dict, Any, List
from motor.motor_asyncio import AsyncIOMotorClient
import os
import pandas as pd

# ---------- 🚀 FastAPI App ----------
app = FastAPI()

# ---------- 🔓 CORS Settings ----------
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # สำหรับ dev
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------- ⚙️ Mongo Setup ----------
MONGO_URI = os.getenv("MONGO_URL")
client = AsyncIOMotorClient(MONGO_URI)

# ---------- 🔄 Serializer ----------
def serialize(item) -> dict:
    item["id"] = str(item["_id"])
    del item["_id"]
    return item

# ---------- 🔧 Collection Resolver ----------
def get_collection(db_name: str, collection_name: str):
    db = client[db_name]
    return db[collection_name]

# ---------- ✅ Insert One ----------
@app.post("/items")
async def create_item(
    item: Item,
    db: str = Query("railway_db"),
    collection: str = Query("items")
):
    col = get_collection(db, collection)
    result = await col.insert_one(item.dict())
    new_item = await col.find_one({"_id": result.inserted_id})
    return serialize(new_item)

# ---------- 📦 Get All Items ----------
@app.get("/items")
async def get_items(
    db: str = Query("railway_db"),
    collection: str = Query("items")
):
    col = get_collection(db, collection)
    items = await col.find().to_list(length=100)
    return [serialize(item) for item in items]

# ---------- 🔍 Query One with JSON ----------
@app.post("/items/query")
async def query_item(
    query: Dict[str, Any] = Body(...),
    db: str = Query("railway_db"),
    collection: str = Query("items")
):
    col = get_collection(db, collection)
    item = await col.find_one(query)
    if not item:
        raise HTTPException(status_code=404, detail="Item not found")
    return serialize(item)

# ---------- 📥 Insert Many ----------
@app.post("/items/bulk")
async def insert_many_items(
    items: List[Dict[str, Any]] = Body(...),
    db: str = Query("railway_db"),
    collection: str = Query("items")
):
    if not items:
        raise HTTPException(status_code=400, detail="No data to insert")

    col = get_collection(db, collection)
    result = await col.insert_many(items)
    inserted_items = await col.find({"_id": {"$in": result.inserted_ids}}).to_list(length=len(result.inserted_ids))
    return [serialize(item) for item in inserted_items]

# ---------- 🌐 Drop & Import from Google Sheet / CSV / Excel ----------
@app.post("/items/reset-and-import")
async def drop_and_import(
    link: str = Body(..., embed=True),
    db: str = Query("railway_db"),
    collection: str = Query("items")
):
    col = get_collection(db, collection)
    await col.drop()

    try:
        if "csv" in link:
            df = pd.read_csv(link,dtype='str')
        elif "xls" in link:
            df = pd.read_excel(link, engine='openpyxl',dtype='str')
        else:
            raise HTTPException(400, "Unsupported file type. Only .csv or .xlsx allowed.")
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error reading file: {str(e)}")

    df = df.fillna("")
    items = df.to_dict(orient="records")
    if not items:
        raise HTTPException(400, detail="No data to insert")

    result = await col.insert_many(items)
    inserted_items = await col.find({"_id": {"$in": result.inserted_ids}}).to_list(length=len(result.inserted_ids))
    return {
        "status": "collection dropped and reloaded",
        "inserted_count": len(inserted_items),
        "sample": [serialize(item) for item in inserted_items[:3]]
    }

# ---------- ✏️ Update One ----------
@app.put("/items/{item_id}")
async def update_item(
    item_id: str,
    item: UpdateItem,
    db: str = Query("railway_db"),
    collection: str = Query("items")
):
    col = get_collection(db, collection)
    update_data = {k: v for k, v in item.dict().items() if v is not None}
    if not update_data:
        raise HTTPException(status_code=400, detail="No fields to update")
    result = await col.update_one({"_id": ObjectId(item_id)}, {"$set": update_data})
    if result.modified_count == 0:
        raise HTTPException(status_code=404, detail="Item not found")
    updated_item = await col.find_one({"_id": ObjectId(item_id)})
    return serialize(updated_item)

# ---------- ❌ Delete One ----------
@app.delete("/items/{item_id}")
async def delete_item(
    item_id: str,
    db: str = Query("railway_db"),
    collection: str = Query("items")
):
    col = get_collection(db, collection)
    result = await col.delete_one({"_id": ObjectId(item_id)})
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Item not found")
    return {"status": "deleted"}

# ---------- 📚 List All Databases ----------
@app.get("/databases")
async def list_databases():
    try:
        dbs = await client.list_database_names()
        return {"databases": dbs}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ---------- 📁 List Collections ----------
@app.get("/collections")
async def list_collections(db: str = Query(...)):
    try:
        db_obj = client[db]
        collections = await db_obj.list_collection_names()
        return {"database": db, "collections": collections}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# -------------------------- 🔧 ADMIN APIs --------------------------

# ✅ View all databases
@app.get("/admin/databases")
async def view_all_databases():
    try:
        dbs = await client.list_database_names()
        return {"databases": dbs}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ❌ Delete a database
@app.delete("/admin/databases/{db_name}")
async def delete_database(db_name: str):
    try:
        await client.drop_database(db_name)
        return {"status": f"Database '{db_name}' deleted successfully."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error deleting database: {str(e)}")

# 📂 List all collections in database
@app.get("/admin/databases/{db_name}/collections")
async def list_collections_in_database(db_name: str):
    try:
        db_obj = client[db_name]
        collections = await db_obj.list_collection_names()
        return {"database": db_name, "collections": collections}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error listing collections: {str(e)}")

# ❌ Delete a collection in a database
@app.delete("/admin/databases/{db_name}/collections/{collection_name}")
async def delete_collection(db_name: str, collection_name: str):
    try:
        await client[db_name][collection_name].drop()
        return {"status": f"Collection '{collection_name}' in database '{db_name}' deleted successfully."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error deleting collection: {str(e)}")
