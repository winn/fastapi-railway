from fastapi import FastAPI, HTTPException, Request, Body
from app.db import collection
from app.models import Item, UpdateItem
from bson import ObjectId
from typing import Dict, Any, List
import pandas as pd

app = FastAPI()

# ---------- 🔄 MongoDB Document Serializer ----------
def serialize(item) -> dict:
    item["id"] = str(item["_id"])
    del item["_id"]
    return item


# ---------- ✅ Insert One ----------
@app.post("/items")
async def create_item(item: Item):
    result = await collection.insert_one(item.dict())
    new_item = await collection.find_one({"_id": result.inserted_id})
    return serialize(new_item)


# ---------- 📦 Get All Items ----------
@app.get("/items")
async def get_items():
    items = await collection.find().to_list(length=100)
    return [serialize(item) for item in items]


# ---------- 🔍 Query One Item with JSON ----------
@app.post("/items/query")
async def query_item(query: Dict[str, Any] = Body(...)):
    item = await collection.find_one(query)
    if not item:
        raise HTTPException(status_code=404, detail="Item not found")
    return serialize(item)


# ---------- 📥 Insert Many ----------
@app.post("/items/bulk")
async def insert_many_items(items: List[Dict[str, Any]] = Body(...)):
    if not items:
        raise HTTPException(status_code=400, detail="No data to insert")
    
    result = await collection.insert_many(items)
    inserted_items = await collection.find({"_id": {"$in": result.inserted_ids}}).to_list(length=len(result.inserted_ids))
    return [serialize(item) for item in inserted_items]


# ---------- 🌐 Import from Google Sheet, CSV, Excel ----------
@app.post("/items/reset-and-import")
async def drop_and_import(link: str = Body(..., embed=True)):
    # 🧨 1. Drop collection
    await collection.drop()

    # 📥 2. Load new data from Google Sheet / CSV / Excel
    try:
        if "csv" in link:
            df = pd.read_csv(link)
        elif "xls" in link:
            df = pd.read_excel(link, engine='openpyxl')
        else:
            raise HTTPException(400, "Unsupported file type. Only .csv or .xlsx allowed.")
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error reading file: {str(e)}")

    df = df.fillna("")
    items = df.to_dict(orient="records")
    if not items:
        raise HTTPException(400, detail="No data to insert")

    result = await collection.insert_many(items)
    inserted_items = await collection.find({"_id": {"$in": result.inserted_ids}}).to_list(length=len(result.inserted_ids))
    return {
        "status": "collection dropped and reloaded",
        "inserted_count": len(inserted_items),
        "sample": [serialize(item) for item in inserted_items[:3]]
    }



# ---------- ✏️ Update One ----------
@app.put("/items/{item_id}")
async def update_item(item_id: str, item: UpdateItem):
    update_data = {k: v for k, v in item.dict().items() if v is not None}
    if not update_data:
        raise HTTPException(status_code=400, detail="No fields to update")
    result = await collection.update_one({"_id": ObjectId(item_id)}, {"$set": update_data})
    if result.modified_count == 0:
        raise HTTPException(status_code=404, detail="Item not found")
    updated_item = await collection.find_one({"_id": ObjectId(item_id)})
    return serialize(updated_item)


# ---------- ❌ Delete One ----------
@app.delete("/items/{item_id}")
async def delete_item(item_id: str):
    result = await collection.delete_one({"_id": ObjectId(item_id)})
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Item not found")
    return {"status": "deleted"}
