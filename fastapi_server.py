from fastapi import FastAPI
from s3_to_pg import load_s3_to_pg, filter_and_save_electronics  # Importing your functions

app = FastAPI()

@app.get("/load-joined-data")
def load_data():
    """Loads the full joined data from S3 into PostgreSQL."""
    try:
        load_s3_to_pg()  # Load full data
        return {"message": "✅ Joined data loaded to PostgreSQL successfully!"}
    except Exception as e:
        return {"error": str(e)}

@app.get("/load-filtered-data")
def load_filtered_data():
    """Loads only the filtered 'Electronics' data into PostgreSQL."""
    try:
        filter_and_save_electronics()  # Load filtered data
        return {"message": "✅ Filtered data loaded to PostgreSQL successfully!"}
    except Exception as e:
        return {"error": str(e)}

# Run the FastAPI server (if executed directly)
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
