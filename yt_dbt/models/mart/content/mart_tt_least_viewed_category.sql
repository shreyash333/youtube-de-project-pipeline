{{ config(materialized='table') }}

-- Model: mart_tt_least_viewed_category
-- Lowest-View Categories : Which categories have the lowest average views across trending videos (Top 10)
-- Aggregates trending videos by category to count
-- Sum up all view_count per category and sort it 

with ranked_videos as (
select
    category_id,                      -- Category of the video
    count(distinct video_id) as videos_count,   -- Number of unique videos in this category
    sum(view_count) as views_count,
     count(distinct country) as country_count    -- Number of unique countries where videos appeared
from {{ref('int_unique_video')}}
group by category_id
)

select top 10 * from ranked_videos order by views_count asc      -- Categories with most videos at top