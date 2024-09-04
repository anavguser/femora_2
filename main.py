# from fastapi import FastAPI, File, UploadFile
# from fastapi.responses import JSONResponse
# import pandas as pd
# import io
# from fastapi.encoders import jsonable_encoder
# from fastapi.middleware.cors import CORSMiddleware

# app = FastAPI()

# @app.api_route("/test-get",methods=["GET"])
# async def test_get():
#     return JSONResponse(content="hello", status_code=200)

# app.add_middleware(
#     CORSMiddleware,
#     allow_origins=["*"],
#     allow_credentials=True,
#     allow_methods=["POST", "GET", "OPTIONS"],
#     allow_headers=["*"],
#     expose_headers=["*"]
# )
    
# @app.post("/process_csv")
# async def process_csv(mtr_file: UploadFile = File(...), payment_file: UploadFile = File(...)):    
#     return JSONResponse(content="hello")

# if __name__ == "__main__":
#     import uvicorn
#     uvicorn.run(app, host="127.0.0.1", port=8000)

from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
import pandas as pd
import numpy as np
import io
import traceback
import json
import os
app = FastAPI()


@app.get("/")
def read_root():
    return {"Hello": "World"}

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods
    allow_headers=["*"],  # Allows all headers
)

def process_mtr(mtr):
    # Step 0: Rename the columns
    mtr = mtr.rename(columns={'Order Id': 'Order ID'})

    # Step 1: Remove rows where 'Transaction Type' is 'Cancel'
    mtr = mtr[mtr['Transaction Type'] != 'Cancel']

    # Step 2: Rename 'Refund' to 'Return'
    mtr['Transaction Type'] = mtr['Transaction Type'].replace('Refund', 'Return')

    # Step 3: Rename 'FreeReplacement' to 'Return'
    mtr['Transaction Type'] = mtr['Transaction Type'].replace('FreeReplacement', 'Return')

    # Step 4: Drop unnecessary columns
    columns_to_drop = ['Invoice Date', 'Shipment Date', 'Shipment Item Id', 'Item Description']
    mtr = mtr.drop(columns=columns_to_drop)

    return mtr

def process_payment(payment):
    # Step 1: Remove rows where 'Type' is 'Transfer'
    payment = payment[payment['type'] != 'Transfer']

    # Step 2: Rename the columns
    column_renames = {
        'type': 'Payment Type',
        'total': 'Net Amount',
        'description': 'P_Description',
        'date/time': 'Payment Date',
        'order id': 'Order ID'
    }
    payment = payment.rename(columns=column_renames)

    # Step 3: Rename specified values to 'Order'
    values_to_rename = ['Adjustment', 'FBA Inventory Fee', 'Fulfilment Fee Refund', 'Service Fee']
    payment['Payment Type'] = payment['Payment Type'].replace(values_to_rename, 'Order')

    # Step 4: Rename 'Refund' to 'Return'
    payment['Payment Type'] = payment['Payment Type'].replace('Refund', 'Return')

    # Step 5: Add a new column 'Transaction Type' and assign 'Payment' to all rows
    payment['Transaction Type'] = 'Payment'

    return payment

def merge_sheets(payment, mtr):
    # Merge the dataframes on 'Order ID' and 'Transaction Type'
    merged_df = pd.merge(payment, mtr, on=['Order ID', 'Transaction Type'], how='outer')

    # Reorder the columns to match the desired output
    columns_order = ['Order ID', 'Transaction Type', 'Payment Type', 'Invoice Amount', 
                     'Net Amount', 'P_Description', 'Order Date', 'Payment Date']
    merged_df = merged_df[columns_order]

    return merged_df

class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return super(NpEncoder, self).default(obj)

@app.post("/api/process_csv")

async def process_csv(mtr_file: UploadFile = File(...), payment_file: UploadFile = File(...)):
    try:
        # Read the CSV files asynchronously
        mtr_content = await mtr_file.read()
        payment_content = await payment_file.read()

        # Convert bytes to StringIO objects
        mtr_io = io.StringIO(mtr_content.decode("utf-8"))
        payment_io = io.StringIO(payment_content.decode("utf-8"))

        # Read the CSV files into pandas DataFrames
        mtr = pd.read_csv(mtr_io)
        payment = pd.read_csv(payment_io)

        # Process MTR and Payment dataframes
        mtr_processed = process_mtr(mtr)
        payment_processed = process_payment(payment)
        

        # Merge the processed dataframes
        result_df = merge_sheets(payment_processed, mtr_processed)

        # Handle NaN values
        result_df = result_df.replace({np.nan: None})

        # Convert the result to a JSON object
        json_result = json.loads(json.dumps(result_df.to_dict(orient="records"), cls=NpEncoder))

        return JSONResponse(content=json_result)
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))

