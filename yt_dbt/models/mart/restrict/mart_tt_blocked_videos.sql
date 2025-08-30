{{ config(materialized='table') }}

-- Model: mart_tt_blocked_videos
-- Description:
-- Most-Blocked Videos : Which trending videos are blocked by the largest number of countries (Top 10)
-- This mart identifies the trending videos that are blocked across multiple countries.  
-- It calculates how many distinct countries have blocked each video and ranks them, keeping 
-- only the highest blocking count per video. This helps in understanding restriction patterns 
-- on trending videos across regions.

with blocked_count as (

    -- Step 1: Count how many distinct countries have blocked each video
    select
        video_id,
        count(distinct block_by_country) as blocked_country_count
    from {{ ref('int_block_country') }}
    group by video_id

),

ranked as (

    -- Step 2: Join the blocked count with video details
    select
        mv.title,  -- all video details from blocked_videos
        mv.channel_title,
        mv.published_at,
        mv.view_count,
        mv.like_count,
        mv.comment_count,
        bc.blocked_country_count
    from {{ ref('int_block_video') }} mv
    join blocked_count bc
        on mv.video_id = bc.video_id
    qualify row_number() over (
        partition by mv.video_id 
        order by bc.blocked_country_count desc
    ) = 1

    
)

-- Final output: list of videos ranked by number of countries blocking them
select top 10 * from ranked order by blocked_country_count desc
