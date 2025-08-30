{{ config(materialized='table') }}

-- Model: mart_tt_blocking_countries
-- Description:
-- Top Blocking Countries : Which countries have blocked the highest number of trending videos (Top 10)
-- This mart identifies the number of trending videos blocked in each country.
-- The result ranks countries by the number of blocked trending videos by that country.

with count_videos as (
    select
    block_by_country,
    count(distinct video_id) as blocked_video_count
    from {{ ref ('int_block_country')}}
    group by block_by_country
    order by blocked_video_count desc
)

select top 10 * from count_videos order by blocked_video_count desc
