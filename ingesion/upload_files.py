"""
YouTube Trending Data → Snowflake Uploader
------------------------------------------

This script automates the process of uploading processed YouTube trending data 
into a Snowflake table. The workflow is:

1. Load RSA private key from file (.ssh/snowflake_rsa_key.p8) for Snowflake authentication.
2. Identify all Parquet files generated for today's date inside `data/processed/YYYY_MM_DD/`.
3. Read all Parquet files into Pandas DataFrames and merge them into a single DataFrame.
4. Establish a secure connection to Snowflake using key-based authentication.
5. Upload the DataFrame into the `YT_DB.RAW.RAW_YOUTUBE_DATA` table.
   - Existing data is **overwritten** (not appended).
6. Print the number of rows successfully inserted.
7. Close the Snowflake connection.

Usage Notes:
- Ensure Parquet files are generated before running this script.
- Requires RSA key-based authentication to be properly configured in Snowflake.
- Dependencies: pandas, pyarrow, snowflake-connector-python, cryptography.

Author: Shreyash Singh
"""

import os
import pandas as pd
from snowflake.connector.pandas_tools import write_pandas
import snowflake.connector
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from datetime import datetime
import configparser

config = configparser.ConfigParser()
config.read("config.cfg")

# Get today's date (used for folder paths, formatted as YYYY_MM_DD)
today = datetime.today().strftime('%Y_%m_%d')

# Load private key for Snowflake authentication
with open(".ssh/snowflake_rsa_key.p8", "rb") as key_file:
    private_key = serialization.load_pem_private_key(
        key_file.read(),
        password=None,                 # No passphrase for this key
        backend=default_backend()
    )

# Placeholder empty DataFrame (not used yet but may help with initialization/debugging)
empty_df = pd.DataFrame()

# Locate all Parquet files from today's processed folder
parquet_folder = f'data/processed/{today}/'
parquet_files = [
    os.path.join(parquet_folder, f)
    for f in os.listdir(parquet_folder)
    if f.endswith(".parquet")          # Pick only Parquet files
]

# If no Parquet files found, stop the script
if not parquet_files:
    raise Exception("No Parquet files found in the directory.")

# Read each Parquet file into a DataFrame, then merge all into one DataFrame
df_list = [pd.read_parquet(pq_file, engine="pyarrow") for pq_file in parquet_files]
full_df = pd.concat(df_list, ignore_index=True)  # Combine all into one DF

# Connect to Snowflake using key-based authentication
conn = snowflake.connector.connect(
    user= config.get("SNOWFLAKE", "user"),                    # Your Snowflake username
    account= config.get("SNOWFLAKE", "account"),              # Snowflake account identifier
    private_key=private_key,                                  # Loaded RSA private key
    role= config.get("SNOWFLAKE", "role"),                    # Role with access
    warehouse= config.get("SNOWFLAKE", "warehouse"),          # Compute warehouse
    database= config.get("SNOWFLAKE", "database"),            # Target database
    schema= config.get("SNOWFLAKE", "schema"),                # Target schema
    insecure_mode=True
)

# Upload DataFrame into Snowflake table using write_pandas
success, nchunks, nrows, _ = write_pandas(
    conn=conn,
    df=full_df,
    table_name="RAW_YOUTUBE_DATA",     # Destination table name
    database= config.get("SNOWFLAKE", "database"), 
    schema= config.get("SNOWFLAKE", "schema"),
    quote_identifiers=True,            # Ensure identifiers are quoted
    overwrite=True                     # Replace table contents (truncate+insert)
)

# Print result summary
print(f"Upload complete. {nrows} rows inserted.")

# Close Snowflake connection
conn.close()
