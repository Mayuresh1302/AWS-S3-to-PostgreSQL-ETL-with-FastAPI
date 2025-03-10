import pandas as pd
import boto3
import io
from sqlalchemy import create_engine, text

# AWS S3 Configuration
AWS_ACCESS_KEY = "AKIA3VRD44Q6QABCRVG3"
AWS_SECRET_KEY = "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"
S3_BUCKET_NAME = "dhira-assignments-bucket-1"
PRODUCTS_FILE_KEY = "products.csv"
CATEGORIES_FILE_KEY = "product_categories.csv"
OUTPUT_FILE_KEY = "joined_products.csv"
FILTERED_FILE_KEY = "filtered_electronics.csv"

# PostgreSQL Configuration
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "postgres"
DB_USER = "postgres"
DB_PASSWORD = "mayureshdbms"
TABLE_NAME = "joined_products"
FILTERED_TABLE_NAME = "filtered_electronics"

# Create an S3 client
s3_client = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY
)

# Function to read CSV from S3
def read_csv_from_s3(file_key):
    response = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=file_key)
    csv_content = response["Body"].read().decode("utf-8")
    return pd.read_csv(io.StringIO(csv_content))

# Load datasets
df_products = read_csv_from_s3(PRODUCTS_FILE_KEY)
df_categories = read_csv_from_s3(CATEGORIES_FILE_KEY)

# Standardize column names
df_products.columns = df_products.columns.str.strip().str.lower()
df_categories.columns = df_categories.columns.str.strip().str.lower()

# Ensure common join key
common_key = "category_name"
if common_key not in df_products.columns or common_key not in df_categories.columns:
    raise KeyError(f"❌ Column '{common_key}' not found in both files!")

# Perform an OUTER JOIN
df_merged = df_products.merge(df_categories, on=common_key, how="outer")

# Save joined file to S3
csv_buffer = io.StringIO()
df_merged.to_csv(csv_buffer, index=False)
s3_client.put_object(Bucket=S3_BUCKET_NAME, Key=OUTPUT_FILE_KEY, Body=csv_buffer.getvalue())

print(f"✅ Joined file saved to S3 as {OUTPUT_FILE_KEY}")

# Function to infer PostgreSQL data types
def infer_pg_type(dtype):
    if pd.api.types.is_integer_dtype(dtype):
        return "INT"
    elif pd.api.types.is_float_dtype(dtype):
        return "NUMERIC"
    elif pd.api.types.is_bool_dtype(dtype):
        return "BOOLEAN"
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return "TIMESTAMP"
    else:
        return "TEXT"

# Function to create table dynamically
def create_table_from_df(engine, table_name, df):
    with engine.connect() as conn:
        col_types = {col.replace(" ", "_"): infer_pg_type(df[col]) for col in df.columns}
        create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} (\n"
        create_table_query += ",\n".join([f"    {col} {col_types[col]}" for col in col_types])
        create_table_query += "\n);"

        conn.execute(text(create_table_query))
        print(f"✅ Table '{table_name}' created successfully!")

# Function to load S3 CSV data into PostgreSQL
def load_s3_to_pg():
    # PostgreSQL connection
    engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

    # Read joined CSV from S3
    df_merged = read_csv_from_s3(OUTPUT_FILE_KEY)

    # Create table dynamically
    create_table_from_df(engine, TABLE_NAME, df_merged)

    # Load data into PostgreSQL
    df_merged.to_sql(TABLE_NAME, engine, if_exists="append", index=False)
    print(f"✅ Data loaded into PostgreSQL table '{TABLE_NAME}'")

# Function to filter Electronics category, save it to S3, and load it into PostgreSQL
def filter_and_save_electronics():
    """Reads joined_products.csv from S3, filters Electronics category, saves back to S3, and loads to PostgreSQL."""
    
    # Read joined CSV from S3
    df = read_csv_from_s3(OUTPUT_FILE_KEY)
    
    # Standardize column names
    df.columns = df.columns.str.strip().str.lower()
    
    # Filter for Electronics category
    filtered_df = df[df["category_name"].str.lower() == "electronics"]
    
    if filtered_df.empty:
        print("❌ No records found for Electronics category.")
        return
    
    # Save filtered data to CSV in memory
    csv_buffer = io.StringIO()
    filtered_df.to_csv(csv_buffer, index=False)
    
    # Upload filtered file to S3
    s3_client.put_object(Bucket=S3_BUCKET_NAME, Key=FILTERED_FILE_KEY, Body=csv_buffer.getvalue())
    
    print(f"✅ Filtered Electronics file saved to S3 as {FILTERED_FILE_KEY}")
    
    # Load filtered data into PostgreSQL
    load_filtered_data_to_postgresql(filtered_df)

# Function to load filtered Electronics data into PostgreSQL
def load_filtered_data_to_postgresql(df):
    """Loads the filtered Electronics data into PostgreSQL."""
    
    # Create PostgreSQL connection
    engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
    
    # Create table dynamically
    create_table_from_df(engine, FILTERED_TABLE_NAME, df)

    # Load data into PostgreSQL
    df.to_sql(FILTERED_TABLE_NAME, engine, if_exists="replace", index=False)
    
    print(f"✅ Filtered Electronics data loaded into PostgreSQL table '{FILTERED_TABLE_NAME}'.")

# Run the functions
if __name__ == "__main__":
    load_s3_to_pg()
    filter_and_save_electronics()
