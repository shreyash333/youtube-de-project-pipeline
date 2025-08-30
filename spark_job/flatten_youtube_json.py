"""
YouTube Trending Data ETL with PySpark
--------------------------------------
This script processes raw YouTube trending video data collected per region (JSON format)
and transforms it into a flattened, structured format (Parquet files).

Steps Performed:
1. Initialize Spark session for distributed processing.
2. Define a list of regions (ISO country codes) for which trending data is available.
3. Loop through each region and:
   - Read raw JSON data from `data/raw/{today}/`.
   - Explode and flatten nested JSON fields such as snippet, statistics, and contentDetails.
   - Extract key attributes: video metadata, channel info, tags, statistics, 
     blocked countries (region restrictions), and additional metadata like load timestamp and country.
4. Save the transformed dataset as a Parquet file in `data/processed/{today}/`
   with one file per region (e.g., `US_trending_YYYY_MM_DD.parquet`).
5. Ensure single-file output per region by writing to a temp directory, renaming, 
   and moving the part file to the target path.

Outcome:
- Produces clean, analytics-ready parquet datasets for each region, 
  which can later be loaded into Snowflake or another warehouse.

Author: Shreyash Singh
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, explode, concat_ws, current_timestamp, lit, when,
    from_json, to_json
)
from datetime import datetime
from pyspark.sql.types import StructType, ArrayType, StringType
import os
import shutil
import glob

# Initialize Spark session
spark = SparkSession.builder \
    .appName("YouTubeTrendingETL") \
    .getOrCreate()

# Current date string for folder and filenames
today = datetime.today().strftime('%Y_%m_%d')

# List of regions (ISO codes) for trending videos
REGIONS = [
    'AR', 'AU', 'AT', 'AZ', 'BH', 'BD', 'BY', 'BE', 'BO', 'BA', 'BR', 'BG', 'CA',
    'CL', 'CO', 'CR', 'HR', 'CY', 'CZ', 'DK', 'DO', 'EC', 'EG', 'SV', 'EE', 'FI',
    'FR', 'GE', 'DE', 'GH', 'GR', 'GT', 'HN', 'HK', 'HU', 'IS', 'IN', 'ID', 'IQ',
    'IE', 'IL', 'IT', 'JM', 'JP', 'JO', 'KZ', 'KE', 'KW', 'LV', 'LB', 'LT', 'LU',
    'MK', 'MY', 'MX', 'ME', 'MA', 'NP', 'NL', 'NZ', 'NI', 'NG', 'NO', 'OM', 'PK',
    'PA', 'PY', 'PE', 'PH', 'PL', 'PT', 'PR', 'QA', 'RO', 'RU', 'SA', 'RS', 'SG',
    'SK', 'SI', 'ZA', 'KR', 'ES', 'LK', 'SE', 'CH', 'TW', 'TZ', 'TH', 'TN', 'TR',
    'UG', 'UA', 'AE', 'GB', 'US', 'UY', 'VE', 'VN', 'YE', 'ZW'
]

# Schema for extracting region restrictions (blocked countries)
region_schema = StructType().add(
    "regionRestriction", StructType().add("blocked", ArrayType(StringType()))
)

# Process each region's raw data
for region in REGIONS:
    input_path = f'data/raw/{today}/{region}_trending_{today}.json'
    output_dir = f'data/processed/{today}/'
    file_name = f'{region}_trending_{today}.parquet'
    output_path = os.path.join(output_dir, file_name)

    # Read raw JSON file
    print(f'Reading {input_path}')
    raw_df = spark.read.option("multiline", "true").json(input_path)
    
    # Explode items array into separate rows
    items_df = raw_df.select(explode(col("items")).alias("item"))

    # Flatten nested fields into tabular structure
    flat_df = items_df.select(
        # Video-level metadata
        col("item.id").alias("video_id"),

        # Snippet fields
        col("item.snippet.publishedAt").alias("published_at"),
        col("item.snippet.channelId").alias("channel_id"),
        col("item.snippet.title").alias("title"),
        col("item.snippet.description").alias("description"),
        col("item.snippet.channelTitle").alias("channel_title"),
        col("item.snippet.categoryId").alias("category_id"),
        col("item.snippet.liveBroadcastContent").alias("live_broadcast_content"),
        col("item.snippet.defaultLanguage").alias("default_language"),
        col("item.snippet.defaultAudioLanguage").alias("default_audio_language"),
        concat_ws(", ", col("item.snippet.tags")).alias("tags"),  # Join tags array into string

        # Content details
        col("item.contentDetails.duration").alias("duration"),
        col("item.contentDetails.dimension").alias("dimension"),
        col("item.contentDetails.definition").alias("definition"),
        col("item.contentDetails.caption").alias("caption"),
        col("item.contentDetails.licensedContent").alias("licensed_content"),

        # Statistics (casted to long for numeric consistency)
        col("item.statistics.viewCount").cast("long").alias("view_count"),
        col("item.statistics.likeCount").cast("long").alias("like_count"),
        col("item.statistics.favoriteCount").cast("long").alias("favorite_count"),
        col("item.statistics.commentCount").cast("long").alias("comment_count"),

        # Region restriction (blocked countries)
        when(
            from_json(to_json(col("item.contentDetails")), region_schema)["regionRestriction"]["blocked"].isNotNull(),
            concat_ws(",", from_json(to_json(col("item.contentDetails")), region_schema)["regionRestriction"]["blocked"])
        ).otherwise(lit(None)).alias("blocked_countries"),

        # Metadata: load timestamp and country code
        current_timestamp().alias("load_ts"),
        lit(region).alias("country")
    )

    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)

    # Write flattened data to a temporary parquet folder
    flat_df.coalesce(1).write.mode("overwrite").parquet("/tmp/temp_parquet")
    
    # Locate the actual parquet part file
    part_file = glob.glob("/tmp/temp_parquet/part-*.parquet")[0]

    # Move parquet file to the final destination with proper name
    shutil.move(part_file, output_path)
    
    # Clean up temporary folder
    shutil.rmtree("/tmp/temp_parquet")
    
    print(f"Flattened data saved to {output_path}")
