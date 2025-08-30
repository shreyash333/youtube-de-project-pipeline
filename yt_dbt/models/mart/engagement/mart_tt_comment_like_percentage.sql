{{ config(materialized='table') }}

-- Model: mart_tt_comment_like_percentage
-- Description:
-- Highest comment-to-like % : Which trending videos have the highest engagement rate (comments per like) (Top 10)
-- This mart calculates the ratio of comments to likes for YouTube trending videos.
-- It filters out videos with null or zero comment_count, ranks rows to keep 

with comment_like as (
    select
    title,
    channel_title,
    published_at,
    category_id,
    like_count,
    comment_count,
    round((nullif(comment_count,0) / like_count) * 100, 2) as comment_like_percentage
from {{ ref('int_unique_video')}}
where comment_count is not null and comment_count <> 0 and like_count is not null and like_count <> 0
order by comment_like_percentage desc)

select top 10 * from comment_like order by comment_like_percentage desc
