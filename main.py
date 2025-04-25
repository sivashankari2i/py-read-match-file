import json
from typing import Dict
from fastapi import FastAPI, File, Form, UploadFile
from fastapi.responses import JSONResponse
import numpy as np
import pandas as pd
from io import BytesIO, StringIO
from pydantic import BaseModel

from create_query import generate_multi_table_queries

app = FastAPI()

entity_schema = {
    "productName": "name of products",
    "parentCategory": "Product's main category",
    "subcategory": "Product's most matching category",
    "identifier": "Unique number for products",
    "description": "About product details",
    "stock": "Quantity detail on the product",
    "MFR": "Reference number",
    "discount":  "Max discount of the product",
    "price":  "Original price"
}

mapping = {
    "productName": ("products", "name"),
    "stock": ("products", "qty"),
    "description": ("products", "description"),
    "identifier": ("products", "identifier"),
    "description": ("products", "description"),
    "MFR": ("products", "MFR"),
    "parentCategory": ("categories", "name"),
    "subcategory": ("categories", "subcategory"),
    "price": ("pricing", "value"),
    "discount":("pricing", "discount")
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
    
    
@app.post("/upload-read-csv/")
async def upload_excel(file: UploadFile = File(...), meta: str = Form(...)   ):
    if not file.filename.endswith(".csv"):
       return JSONResponse(status_code=400, content={"error": "Only CSV files are supported."})

    params = json.loads(meta)
    contents = await file.read()
    decoded_contents = contents.decode('utf-8')  # Decode bytes to string
    csv_file = StringIO(decoded_contents)
    try:
        # Read Excel content into pandas DataFrame
        print("reading decoded_contents")
        df = pd.read_csv(csv_file, usecols=params.keys())
        df.rename(columns=params, inplace=True)
        data = df.to_dict(orient="records")
        # matched, suggestions, unmatched, unmatchedEntityList, matchedEntityList = match_excel_headers(excel_headers, entity_schema)

 
        return data
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

    
@app.post("/upload-read-excel/")
async def upload_excel(file: UploadFile = File(...), meta: str = Form(...)   ):
    if not file.filename.endswith((".xlsx", ".xls")):
        return JSONResponse(status_code=400, content={"error": "Invalid file format. Please upload an Excel file."})

    params = json.loads(meta)
    contents = await file.read()
    print("reading eleeeeeeeeeeeee")
    try:
        # Read Excel content into pandas DataFrame
        print("reading decoded_contents")
        df = pd.read_excel(BytesIO(contents), usecols=params.keys())
        # df = df.replace([np.nan, np.inf, -np.inf], None)
        print("After read decoded_contents changes")
        df.rename(columns=params, inplace=True)
        
        print("After re formatting contents changes")
        data = df.to_dict(orient="records")
        invalid_columns = df.columns[df.isin([np.nan, np.inf, -np.inf]).any()].tolist()

        # Clean data for JSON response
        df_clean = df.replace([np.nan, np.inf, -np.inf], None)

        # Convert to dict for JSON-safe return
        data = df_clean.to_dict(orient="records")

        return JSONResponse(content={
            "data": data,
            "invalid_columns": invalid_columns
        })
        # matched, suggestions, unmatched, unmatchedEntityList, matchedEntityList = match_excel_headers(excel_headers, entity_schema)

 
        # return df
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

    
@app.post("/create-query-excel/")
async def upload_excel(file: UploadFile = File(...), meta: str = Form(...) ):
    if not file.filename.endswith((".xlsx", ".xls")):
        return JSONResponse(status_code=400, content={"error": "Invalid file format. Please upload an Excel file."})

    contents = await file.read()
    print("reading eleeeeeeeeeeeee")
    try:
        params = json.loads(meta)
        # Read Excel content into pandas DataFrame
        print("reading decoded_contents")
        df = pd.read_excel(BytesIO(contents), usecols=params.keys())
        df = df.replace([np.nan, np.inf, -np.inf], None)
        print("After read decoded_contents changes")
        df.rename(columns=params, inplace=True)
        data = df.to_dict(orient="records")
        print("After re formatting contents changes")

        # Convert to dict for JSON-safe return
        queries_by_table = generate_multi_table_queries(df, mapping)
        for table, queries in queries_by_table.items():
            print(f"\n-- {table.upper()} --")
            for q, v in queries:
                print(q)
                print("Values:", v)
        # matched, suggestions, unmatched, unmatchedEntityList, matchedEntityList = match_excel_headers(excel_headers, entity_schema)

 
        return {"queries_by_table": queries_by_table,
                "data": data}
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})
 
def generate_multi_table_queries(df, mapping):

    table_queries = defaultdict(list)

    for _, row in df.iterrows():
        temp_rows = defaultdict(dict)

        for excel_col, target in mapping.items():
            value = row.get(excel_col)
            if isinstance(target, tuple) and isinstance(target[1], dict):
                # Nested fields
                table, sub_map = target
                for sub_key, db_field in sub_map.items():
                    val = value.get(sub_key) if isinstance(value, dict) else None
                    temp_rows[table][db_field] = val
            elif isinstance(target, tuple):
                table, field = target
                temp_rows[table][field] = value

        # Build queries for each table
        for table, row_data in temp_rows.items():
            columns = ", ".join(row_data.keys())
            placeholders = ", ".join(["%s"] * len(row_data))
            values = tuple(row_data.values())
            query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders});"
            table_queries[table].append((query, values))

    return table_queries
