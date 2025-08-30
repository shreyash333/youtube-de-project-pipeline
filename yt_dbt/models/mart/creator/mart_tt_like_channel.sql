{{ config(materialized='table') }}

-- Model: mart_tt_like_channel
-- Highest-liked Channels : Which channels have the highest total likes (Top 10)
-- This mart identifies the top 10 YouTube channels with the highest total likes  
-- across all trending videos worldwide.  
-- Steps:
--   1. Rank each video by like_count within video_id to ensure only the top record is kept.  
--   2. Aggregate results by channel to calculate:
--        - Number of unique trending videos
--        - Total like volume across all videos
--   3. Select the top 10 channels with the highest likes_count.

with channel_likes as (

    select
        channel_title,
        count(distinct video_id) as videos_count,
        sum(like_count) as likes_count
    from {{ ref('int_unique_video')}}
    where like_count is not null
    group by channel_id, channel_title
)

-- Step 3: Select top 10 channels by likes_count
select top 10 * from channel_likes order by likes_count desc
