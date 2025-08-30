-- models/intermediate/int_unique_video.sql
{{ config(
    materialized='view'
) }}

-- Model: int_unique_video
-- Description:
--   This intermediate model ranks YouTube videos by `like_count` within each `video_id`.
--   The goal is to remove duplicates or conflicting records by keeping the "best" row
--   (i.e., the one with the highest like count).
--
--   Key steps:
--   - Reads from the staging model `stg_youtube_data`.
--   - Applies ROW_NUMBER() window function partitioned by `video_id`.
--   - Orders by `like_count` descending, so the top-liked record per video_id is ranked as 1.
--   - Filters only `rn1 = 1` so that only the top record per video_id is kept.

with ranked_videos as (
    select
        yt.*,
        row_number() over (
            partition by video_id
            order by like_count desc
        ) as rn1
    from {{ ref('stg_youtube_data') }} yt
)

select *
from ranked_videos
where rn1 = 1
