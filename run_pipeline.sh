#!/bin/bash
set -e   # Exit immediately if a command fails

# Build docker image
docker build -t youtube-project .

# Ingest YouTube trending data
docker run -it --rm -v "$(pwd):/app" -v "$(pwd)/.dbt:/root/.dbt" -v "$(pwd)/.ssh:/root/.ssh" -w /app youtube-project python3 ingestion/download_yt_data.py

# Flatten JSON data using Spark
docker run -it --rm -v "$(pwd):/app" -v "$(pwd)/.dbt:/root/.dbt" -v "$(pwd)/.ssh:/root/.ssh" -w /app youtube-project spark-submit spark_job/flatten_youtube_json.py

# Upload files to Snowflake
docker run -it --rm -v "$(pwd):/app" -v "$(pwd)/.dbt:/root/.dbt" -v "$(pwd)/.ssh:/root/.ssh" -w /app youtube-project python3 ingestion/upload_files.py

# Run dbt debug to test connection
docker run -it --rm -v "$(pwd):/app" -v "$(pwd)/.dbt:/root/.dbt" -v "$(pwd)/.ssh:/root/.ssh" -w /app youtube-project dbt debug

# Run dbt models
docker run -it --rm -v "$(pwd):/app" -v "$(pwd)/.dbt:/root/.dbt" -v "$(pwd)/.ssh:/root/.ssh" -w /app youtube-project dbt run --select stg_youtube_data+ --profiles-dir /root/.dbt

# Generate PDF report
docker run -it --rm -v "$(pwd):/app" -v "$(pwd)/.dbt:/root/.dbt" -v "$(pwd)/.ssh:/root/.ssh" -w /app youtube-project python3 report/create_report.py

echo "✅ Pipeline execution completed successfully!"
