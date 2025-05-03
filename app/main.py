from fastapi import FastAPI, HTTPException
from app.db import collection
from app.models import Item, UpdateItem
from bson import ObjectId

app = FastAPI()

def serialize(item) -> dict:
    item["id"] = str(item["_id"])
    del item["_id"]
    return item

@app.post("/items")
async def create_item(item: Item):
    result = await collection.insert_one(item.dict())
    new_item = await collection.find_one({"_id": result.inserted_id})
    return serialize(new_item)

@app.get("/items")
async def get_items():
    try:
        items = await collection.find().to_list(length=100)
        return items
    except Exception as e:
        print("❌ Error fetching items:", str(e))
        raise HTTPException(status_code=500, detail=str(e))



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

@app.delete("/items/{item_id}")
async def delete_item(item_id: str):
    result = await collection.delete_one({"_id": ObjectId(item_id)})
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Item not found")
    return {"status": "deleted"}

