{{ config(materialized='table') }}

-- Model: mart_tt_long_videos
-- Longest Trending Videos : Which trending videos have the longest durations (Top 10)
-- Selects the longest videos in trending data by ordering on duration,
-- after ensuring only one record per video_id is kept (highest like_count).

with ranked_videos as (
    select          
    title,              -- Video title
    published_at,       -- Publish timestamp
    channel_title,      -- Channel display name
    category_id,        -- Video category
    duration          -- Video duration (ISO 8601 format, e.g. PT15M33S)
from {{ref('int_unique_video')}}  
)

select top 10 * from ranked_videos order by duration desc         -- Ordering videos by length (longest first)

