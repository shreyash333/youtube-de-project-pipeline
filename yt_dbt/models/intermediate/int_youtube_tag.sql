-- models/intermediate/int_youtube_tag.sql
{{ config(
    materialized='view'
) }}

-- Model: int_youtube_tag
-- Description:
--   This intermediate model normalizes the `tags` column from `stg_youtube_data`
--   by splitting the array/string of tags into individual rows.
--   Each row corresponds to a single video-tag-country combination,
--   which makes downstream analysis (e.g., top tags per country or time period)
--   much simpler and more efficient.

with base as (
    -- Select only the fields needed for tag normalization
    select
        video_id,
        load_ts,
        country,
        tags
    from {{ ref('stg_youtube_data') }}
),

yt_tag as (
    -- Use the split_to_rows macro to expand tags into multiple rows
    select
        video_id,
        trim(t.value::string) as tag,   -- Clean each tag by trimming whitespace
        load_ts,
        country
    from base,
    {{ split_to_rows('tags') }}        -- Macro that splits tags into row set
    where trim(t.value::string) is not null
      and trim(t.value::string) <> ''  -- Filter out empty or null tags
)

-- Final output: one row per (video_id, tag, country, load_ts)
select *
from yt_tag
