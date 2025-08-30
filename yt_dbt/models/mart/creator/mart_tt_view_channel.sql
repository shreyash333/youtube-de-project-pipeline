{{ config(materialized='table') }}

-- Model: mart_tt_view_channel
-- Highest-viewed Channels : Which channels have the highest total views (Top 10)
-- This mart identifies the top 10 YouTube channels with the highest total views  
-- across all trending videos worldwide.  
-- Steps:
--   1. Rank each video by view_count within video_id to ensure only the top record is kept.  
--   2. Aggregate results by channel to calculate:
--        - Number of unique trending videos
--        - Total view volume across all videos
--   3. Select the top 10 channels with the highest views_count.

with channel_views as (

    select
        channel_title,
        count(distinct video_id) as videos_count,
        sum(view_count) as views_count
    from {{ ref('int_unique_video')}}
    where view_count is not null
    group by channel_id, channel_title
)

-- Step 3: Select top 10 channels by views_count
select top 10 * from channel_views order by views_count desc
