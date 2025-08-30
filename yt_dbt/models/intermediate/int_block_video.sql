-- models/intermediate/int_blocked_videos.sql
{{ config(
    materialized='view'
) }}

-- Model: int_blocked_videos
-- Description:
--   This intermediate model identifies YouTube videos that are blocked in at least one country.
--   It filters out rows with NULL or empty blocked_countries, then deduplicates to avoid duplicates.
--
--   Key steps:
--   - Reads from the staging model stg_youtube_data.
--   - Filters only rows where blocked_countries is present.
--   - Deduplicates using DISTINCT.
--   - Orders results by published_at descending.

with blocked_videos as (
    select
        video_id,
        country,
        title,
        channel_title,
        published_at,
        view_count,
        like_count,
        comment_count,
        tags,
        blocked_countries,
        category_id,
        load_ts
    from {{ ref('stg_youtube_data') }}
    where blocked_countries is not null
      and blocked_countries <> ''
),

deduped as (
    select distinct *
    from blocked_videos
)

select *
from deduped
