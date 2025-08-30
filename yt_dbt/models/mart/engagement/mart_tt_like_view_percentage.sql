{{ config(materialized='table') }}

-- Model: mart_tt_like_view_percentage
-- Description:
-- Highest Like-to-View % : Which trending videos have the highest engagement rate (likes per view) (Top 10)
-- This mart calculates the ratio of likes to views for YouTube trending videos.
-- It filters out videos with null or zero like_count, ranks rows to keep 

with like_view as (
    select
    title,
    channel_title,
    published_at,
    category_id,
    view_count,
    like_count,
    round((nullif(like_count,0) / view_count) * 100, 2) as like_view_percentage
from {{ ref('int_unique_video')}}
where like_count is not null and like_count <> 0
order by like_view_percentage desc)

select top 10 * from like_view order by like_view_percentage desc
