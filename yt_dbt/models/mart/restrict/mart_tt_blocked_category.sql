{{ config(materialized='table') }}

-- Model: mart_tt_blocked_category
-- Description:
-- Blocked Categories : Which video categories are blocked the most (Top 10)
-- This model calculates the number of unique blocked videos per category.
-- It aggregates from the `int_block_video` mart and returns a category-level summary
-- showing how many videos are blocked in each category.

WITH blocked_count AS (
    SELECT
        category_id,
        COUNT(DISTINCT video_id) AS blocked_video_count
    FROM {{ ref('int_block_video') }}
    GROUP BY category_id
    ORDER BY blocked_video_count DESC
)

-- Final selection: blocked video counts by category
SELECT top 10 * FROM blocked_count order by blocked_video_count desc
