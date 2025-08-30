{{ config(materialized='table') }}

-- Model: mart_tt_short_videos
-- Shortest Trending Videos : Which trending videos have the Shortest durations (Top 10)
-- Selects the Shortest videos in trending data by ordering on duration,
-- after ensuring only one record per video_id is kept (highest like_count).

with ranked_videos as (
    select          
    title,              -- Video title
    published_at,       -- Publish timestamp
    channel_title,      -- Channel display name
    category_id,        -- Video category
    duration          -- Video duration 
from {{ref('int_unique_video')}}  
)

select top 10 * from ranked_videos order by duration asc         -- Ordering videos by length (Shortest first)

