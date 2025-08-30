{{ config(materialized='table') }}

-- Model: mart_tt_most_used_category
-- Most Used Categories : Which categories appear most frequently in trending videos (Top 10)
-- Aggregates trending videos by category to count
-- how many unique videos and how many countries they appear in.

with ranked_videos as (
select
    category_id,                      -- Category of the video
    count(distinct video_id) as videos_count,   -- Number of unique videos in this category
    count(distinct country) as country_count    -- Number of unique countries where videos appeared
from {{ref('int_unique_video')}}
group by category_id
)

select top 10 * from ranked_videos order by videos_count desc      -- Categories with most videos at top