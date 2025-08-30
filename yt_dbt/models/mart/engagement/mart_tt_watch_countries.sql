{{ config(materialized='table') }}

-- Model: mart_tt_watch_countries
-- Top Watching Countries : Which countries watch the most trending videos (Top 10)
-- Summarizes YouTube trending data at the country level.
-- For each country, we calculate:
--   1) unique_trending_videos → how many distinct videos trended.
--   2) total_views → cumulative views across all trending videos.
--   3) view_rank → rank countries by total views (highest = 1).
--   4) video_rank → rank countries by number of unique trending videos (highest = 1).
-- Finally, order results by view_rank (so most-viewed countries appear on top).

with top_watch_country as (
    SELECT
    country,
    COUNT(DISTINCT video_id) AS unique_trending_videos,  -- number of unique videos trended in this country
    SUM(view_count) AS total_views,                      -- total views across all trending videos in this country

    -- rank countries based on total views (1 = highest total views)
    RANK() OVER (ORDER BY SUM(view_count) DESC) AS view_rank,

    -- rank countries based on unique trending videos (1 = most unique videos trended)
    RANK() OVER (ORDER BY COUNT(DISTINCT video_id) DESC) AS video_rank

FROM {{ ref('stg_youtube_data') }}
GROUP BY country
ORDER BY view_rank  -- show countries ranked by total views
)

select top 10 * from top_watch_country order by view_rank desc, video_rank desc

