{{ config(materialized='table') }}

-- Model: mart_tt_viewed_videos
-- Description:
--Highest-View Videos : Which trending videos have the highest view counts (Top 10)
-- Identifies the Top 10 trending videos (by view_count) for each country.
-- Steps:
--   1) ranked_videos: Within each video_id, rank records by view_count (rn1 = 1 keeps max-view row).
--   2) rank_by_country: For each country, rank videos by view_count (rn2).
--   3) top_videos: Filter to keep only the Top 10 (rn2 <= 10) per country.
--   4) Final select: Output full details of these top trending videos.

WITH rank_by_country AS (
    SELECT
        rv.*,
        ROW_NUMBER() OVER (
            PARTITION BY country
            ORDER BY view_count DESC   -- rank videos within each country by views
        ) AS rn2 
    FROM {{ ref('int_unique_video')}} rv
    WHERE view_count IS NOT NULL  -- exclude videos without views
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
    view_count
    FROM top_videos

)

select top 10 * from final_block order by view_count desc
