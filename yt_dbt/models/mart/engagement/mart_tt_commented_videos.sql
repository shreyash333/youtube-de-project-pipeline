{{ config(materialized='table') }}

-- Model: mart_tt_commented_videos
-- Description:
--Highest-comment Videos : Which trending videos have the highest comment counts (Top 10)
-- Identifies the Top 10 trending videos (by comment_count) for each country.
-- Steps:
--   1) ranked_videos: Within each video_id, rank records by comment_count (rn1 = 1 keeps max-comment row).
--   2) rank_by_country: For each country, rank videos by comment_count (rn2).
--   3) top_videos: Filter to keep only the Top 10 (rn2 <= 10) per country.
--   4) Final select: Output full details of these top trending videos.

WITH rank_by_country AS (
    SELECT
        rv.*,
        ROW_NUMBER() OVER (
            PARTITION BY country
            ORDER BY comment_count DESC   -- rank videos within each country by comments
        ) AS rn2 
    FROM {{ ref('int_unique_video')}} rv
    WHERE comment_count IS NOT NULL  -- exclude videos without comments
),
top_videos AS (
    SELECT *
    FROM rank_by_country
    WHERE rn2 <= 10                     -- top 10 videos per country      
),
final_block as (
    SELECT
    title,
    published_at,
    channel_title,
    category_id,
    comment_count
    FROM top_videos

)

select top 10 * from final_block order by comment_count desc
