from fastapi import FastAPI, File, UploadFile
from fastapi.responses import JSONResponse
import pandas as pd
from io import BytesIO, StringIO
from pydantic import BaseModel

app = FastAPI()

entity_schema = {
    "productName": "name of products",
    "parentCategory": "Product's main category",
    "Subcategory": "Product's most matching category",
    "identifier": "Unique number for products",
    "Description": "About product details",
    "qty": "Quantity detail on the product",
    "MFR.": "Reference number",
    "Price": {
        "value": "Original price",
        "discount": "Min discount",
        "maxDiscount": "Max discount"
    }
}

# Define a Pydantic model for the POST request body
class Item(BaseModel):
    name: str
    description: str | None = None
    price: float
    quantity: int

# Root endpoint
@app.get("/")
def read_root():
    return {"message": "Welcome to the FastAPI demo!"}

# POST endpoint
@app.post("/items/")
def create_item(item: Item):
    total_cost = item.price * item.quantity
    return {
        "name": item.name,
        "description": item.description,
        "total_cost": total_cost
    }

@app.post("/upload-csv/")
async def upload_excel(file: UploadFile = File(...)):
    if not file.filename.endswith(".csv"):
       return JSONResponse(status_code=400, content={"error": "Only CSV files are supported."})
    # if not file.filename.endswith((".xlsx", ".xls")):
    #     return JSONResponse(status_code=400, content={"error": "Invalid file format. Please upload an Excel file."})

    contents = await file.read()
    print(contents)
    decoded_contents = contents.decode('utf-8')  # Decode bytes to string
    csv_file = StringIO(decoded_contents)
    print("reading eleeeeeeeeeeeee")
    try:
        # Read Excel content into pandas DataFrame
        print("reading decoded_contents")
        # df = pd.read_csv(BytesIO(csv_file))
        print("reading csv_file", csv_file)
        df = pd.read_csv(csv_file, nrows=0)
        print("readed decoded_contents", df)
        return { "preview":   df.columns.tolist()}
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})
    
@app.post("/upload-excel/")
async def upload_excel(file: UploadFile = File(...)):
    if not file.filename.endswith((".xlsx", ".xls")):
        return JSONResponse(status_code=400, content={"error": "Invalid file format. Please upload an Excel file."})

    contents = await file.read()
    print("reading eleeeeeeeeeeeee")
    try:
        # Read Excel content into pandas DataFrame
        print("reading decoded_contents")
        df = pd.read_excel(BytesIO(contents), nrows=0)
        excel_headers = df.columns.tolist()
        matched, suggestions, unmatched, unmatchedEntityList, matchedEntityList = match_excel_headers(excel_headers, entity_schema)

 
        return {"matched": matched,
                "suggestions": suggestions,
                "unmatched": unmatched,
                "unmatchedEntityList": unmatchedEntityList,
                "matchedEntityList": matchedEntityList}
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})
    
def match_excel_headers(excel_headers, entity_schema):
    def normalize(s):
        return s.strip().lower().replace(" ", "")

    def flatten_entity(schema, prefix=""):
        fields = {}
        for key, value in schema.items():
            full_key = f"{prefix}.{key}" if prefix else key
            if isinstance(value, dict):
                fields.update(flatten_entity(value, full_key))
            else:
                fields[normalize(full_key)] = full_key
        return fields

    # Flatten the entity schema for comparison
    flat_entity_fields = flatten_entity(entity_schema)

    matched = {}
    unmatched = []
    suggestions = {}
    unmatchedEntityList = flat_entity_fields
    matchedEntityList = []

    for header in excel_headers:
        key = normalize(header)

        if key in flat_entity_fields:
            matched[header] = flat_entity_fields[key]
            matchedEntityList.append(key)
            # unmatchedEntityList.remove(key)
        else:
            # Try fuzzy match / partial match
            guess = next((v for k, v in flat_entity_fields.items() if key in k), None)
            if guess:
                suggestions[header] = guess
                matchedEntityList.append(key)
                # unmatchedEntityList.remove(key)
            else:
                unmatched.append(header)

    print("=matchedEntityList===", matchedEntityList)
    # for m in matchedEntityList:
        # unmatchedEntityList.pop(m)
    return matched, suggestions, unmatched, unmatchedEntityList, matchedEntityList