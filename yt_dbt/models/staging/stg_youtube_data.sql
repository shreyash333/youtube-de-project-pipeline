-- models/staging/stg_youtube_data.sql
{{ config(
    materialized='view'
) }}

-- Model: stg_youtube_data
-- Description:
--   This staging model standardizes and cleans data from the raw YouTube source table.
--   It ensures column names are normalized, applies transformations through macros
--   (e.g., published_at, duration, and load_ts conversions), and prepares the dataset
--   for downstream intermediate and mart models.
--
--   Key steps:
--   - Pulls all raw columns from `youtube_data.raw_youtube_data`.
--   - Renames columns using consistent formatting and casing.
--   - Applies type/format conversions using dbt macros for timestamps and durations.
--   - Outputs a clean, analysis-ready view of YouTube trending video data.

with source_table as (
    -- Load all data from the raw YouTube source table
    select * 
    from {{ source('youtube_data', 'raw_youtube_data') }}
),

renamed as (
    -- Apply transformations and standardize column naming
    select
        "video_id" as video_id,
        {{ convert_published_at("published_at") }} as published_at,  -- convert string timestamp to proper datetime
        "channel_id" as channel_id,
        "title" as title,
        "description" as description,
        "channel_title" as channel_title,
        "category_id" as category_id,
        "live_broadcast_content" as live_broadcast_content,
        "default_language" as default_language,
        "default_audio_language" as default_audio_language,
        "tags" as tags,
        {{ convert_duration("duration") }} as duration,              -- convert ISO 8601 duration to seconds
        "dimension" as dimension,
        "definition" as definition,
        "caption" as caption,
        "licensed_content" as licensed_content,
        "view_count" as view_count,
        "like_count" as like_count,
        "favorite_count" as favorite_count,
        "comment_count" as comment_count,
        "blocked_countries" as blocked_countries,
        {{ convert_load_ts("load_ts") }} as load_ts,                 -- convert ingestion timestamp to proper type
        "country" as country
    from source_table
)

-- Final clean dataset
select *
from renamed
