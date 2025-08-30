{{ config(materialized='table') }}

-- Model: mart_tt_comment_channel
-- Highest-Commented Channels : Which channels have the highest total comments (Top 10)
-- This mart identifies the top 10 YouTube channels with the highest total comments  
-- across all trending videos worldwide.  
-- Steps:
--   1. Rank each video by comment_count within video_id to ensure only the top record is kept.  
--   2. Aggregate results by channel to calculate:
--        - Number of unique trending videos
--        - Total comment volume across all videos
--   3. Select the top 10 channels with the highest comments_count.

with channel_comments as (

    select
        channel_title,
        count(distinct video_id) as videos_count,
        sum(comment_count) as comments_count
    from {{ ref('int_unique_video')}}
    where comment_count is not null
    group by channel_id, channel_title
)

-- Step 3: Select top 10 channels by comments_count
select top 10 * from channel_comments order by comments_count desc
