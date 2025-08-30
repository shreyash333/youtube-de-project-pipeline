{{ config(materialized='table') }}

-- Model: mart_tt_comment_view_percentage
-- Description:
-- Highest comment-to-View % : Which trending videos have the highest engagement rate (comments per view) (Top 10)
-- This mart calculates the ratio of comments to views for YouTube trending videos.
-- It filters out videos with null or zero comment_count, ranks rows to keep 

with comment_view as (
    select
    title,
    channel_title,
    published_at,
    category_id,
    view_count,
    comment_count,
    round((nullif(comment_count,0) / view_count) * 100, 2) as comment_view_percentage
from {{ ref('int_unique_video')}}
where comment_count is not null and comment_count <> 0
order by comment_view_percentage desc)

select top 10 * from comment_view order by comment_view_percentage desc
