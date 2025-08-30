# YouTube Trending Data Engineering Project Report

## 1. Executive Summary

This project demonstrates the design and implementation of an end-to-end data engineering pipeline to process, transform, and analyze YouTube Trending Videos data.
The pipeline automates data ingestion from the YouTube API, processes raw data with PySpark inside Docker, stores it in Snowflake, applies transformations with dbt, and visualizes insights in Power BI dashboards.

The solution showcases scalable, production-grade practices used in modern data engineering workflows.

---

## 2. Tools & Technologies

* Ingestion: Python, YouTube Data API
* Processing: PySpark (running inside Docker container)
* Data Storage: Parquet files, Snowflake Data Warehouse
* Transformations: dbt (SQL + macros + views)
* Visualization: Power BI
* Orchestration/Infrastructure: Docker

---

## 3. Folder Structure

├── config.cfg                          # Global project configuration file
├── data_pipeline_achitecture.png       # Architecture diagram of the data pipeline
├── Dockerfile                          # Docker image definition for containerizing the 
├── README.MD                           # Project documentation and setup guide
├── requirements.txt                    # Python dependencies for the pipeline
├── run.txt                             # Run notes or sample commands
├── run_pipeline.sh                     # Shell script to orchestrate the entire pipeline
├── .ssh                                # Secure folder (stores SSH/RSA keys if needed)
│
├── data                                # Data storage directory
│   ├── processed                       # Parquet files after Spark/dbt transformations
│   └── raw                             # Raw JSON files directly from YouTube API
│
├── ingesion                            # Data ingestion scripts
│   ├── download_yt_data.py             # Script to download trending YouTube data from API
│   └── upload_files.py                 # Script to upload raw files to Snowflake/Storage
│
├── logs                                # Log files directory
│   └── dbt.log                         # dbt execution logs for debugging
│
├── powerBI                             # Power BI reports and dashboards
│   ├── Youtube.pbix                    # Power BI project file
│   └── Youtube_PowerBI_report.pdf      # Exported PDF version of the dashboard
│
├── report                              # Automated reporting scripts and outputs
│   ├── create_report.py                # Script to generate reports
│   └── youtube_trending_report_2025-08-29.pdf  # Example generated report
│
├── snowflake                           # Snowflake database scripts
│   └── ddls.sql                        # DDL statements for schema/tables creation
│
├── spark_job                           # Spark transformation jobs
│   └── flatten_youtube_json.py         # Script to flatten nested YouTube JSON into tabular format
│
└── yt_dbt                              # dbt project for transformations
    ├── .dbt
    │   ├── .user.yml                   # User-specific dbt configs
    │   └── profiles.yml                # Connection configs for dbt (Snowflake, etc.)
    │
    ├── .ssh
    │   └── snowflake_rsa_key.p8        # RSA private key for Snowflake authentication
    │
    ├── macros                          # Custom dbt macros (reusable SQL snippets/functions)
    ├── models                          # dbt models (SQL transformations)
    │   ├── intermediate                # Complex transformation logic before marts
    │   ├── mart                        # Final business-ready tables
    │   │   ├── content                 # Content-focused metrics (titles, categories, etc.)
    │   │   ├── creator                 # Creator/channel-based analytics
    │   │   ├── engagement              # Engagement metrics (views, likes, comments)
    │   │   ├── history                 # Historical snapshots of trending data
    │   │   └── restrict                # Restricted/blocked video analysis
    │   └── staging                     # Raw data cleaning and standardization
    │
    ├── snapshots                       # dbt snapshots for SCD (slowly changing dimensions)
    ├── staging                         # Temporary staging scripts/tables if needed
    └── tests                           # dbt tests for data quality

## 4. Architecture

The pipeline follows this flow:

YouTube API → Python Ingestion → PySpark (Docker) → Parquet Files → Snowflake Staging → dbt Transformations → Snowflake Final Tables → Power BI Dashboards → PDF reports

---

## 5. Schema Documentation

### 5.1 Staging Schema (stg\_youtube\_data)

* video\_id (STRING): Unique YouTube video ID
* title (STRING): Video title
* description (STRING): Video description
* channel\_id (STRING): Channel ID
* channel\_title (STRING): Channel name
* category\_id (INT): Video category
* published\_at (TIMESTAMP): Published date/time
* view\_count (INT): Total views
* like\_count (INT): Total likes
* comment\_count (INT): Number of comments
* tags (ARRAY): Video tags
* country (STRING): Country code
* blocked\_countries (ARRAY): Countries where blocked
* trending\_date (DATE): Date of trending snapshot

### 5.2 Transformed Schema (fct\_trending\_videos)

* video\_id (STRING): Unique video ID
* title (STRING): Cleaned video title
* channel\_id (STRING): Channel ID
* channel\_title (STRING): Channel name
* category (STRING): Mapped video category
* published\_at (TIMESTAMP): Published date
* trending\_date (DATE): Trending date
* view\_count (INT): Latest views at trending snapshot
* like\_count (INT): Latest likes
* comment\_count (INT): Latest comments
* country (STRING): Country code
* is\_live (BOOLEAN): Whether the video is live
* is\_blocked (BOOLEAN): True if blocked in one or more countries
* blocking\_count (INT): Number of countries blocking the video

---

## 6. Data Flow & Transformations

1. **Ingestion (Python + YouTube API)**

   * Fetch trending videos per country
   * Store raw JSON responses

2. **Processing (PySpark inside Docker)**

   * Flatten nested JSON
   * Handle null/missing values
   * Store intermediate data as Parquet

3. **Storage (Snowflake Staging)**

   * Load Parquet files into staging tables
   * Maintain raw data integrity

4. **Transformations (dbt)**

   * Create cleaned, analytics-ready models
   * Implement macros and reusable SQL logic
   * Maintain historical trending snapshots

5. **Visualization (Power BI)**

   * Connect to Snowflake
   * Build dashboards for:

     * Top channels by views/likes
     * Country-level insights
     * Video category analysis
     * Blocking/restriction patterns

---

## 7. Dashboards (Examples)

* Top channels by engagement metrics
* Country-wise trending patterns
* Category popularity analysis
* Blocked videos and restrictions

---

## 8. Challenges & Learnings

* Handling nested YouTube API JSON structure in Spark
* Setting up Snowflake RSA key-based authentication in Docker
* Scaling ingestion for 200+ countries daily
* Designing time-series analysis for historical trending data

---

## 9. Future Enhancements

* Automate pipeline with Apache Airflow
* Implement real-time streaming ingestion (Kafka + Spark Streaming)
* Deploy dashboards as a web application
* Extend to machine learning models for video popularity prediction

---

## 10. Conclusion

This project demonstrates how modern data engineering practices can be applied to build a scalable, reliable, and insightful analytics pipeline.
It highlights practical integration of APIs, big data processing, cloud data warehousing, transformations, and BI tools into a unified solution.

