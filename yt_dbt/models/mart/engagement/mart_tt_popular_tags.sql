{{ config(materialized='table') }}

-- Model: mart_tt_popular_tags
-- Description:
-- Popular Tags : Which tags appear most often in trending videos (Top 10)
--   This model identifies the **top 100 most frequently used tags per country** 
--   for each load snapshot. It breaks apart comma-separated tags, counts their 
--   frequency, and ranks them per country to show trending keywords.
-- Use case:
--   Useful for analyzing trending topics, cultural differences in tags, and 
--   monitoring how tag popularity shifts over time.


with tag_counts AS (
    -- Step 2: Count how many times each tag appears per country + load_ts
    SELECT
        country,
        tag,
        load_ts,
        COUNT(*) AS tag_count
    FROM {{ ref('int_youtube_tag') }},
    WHERE tag IS NOT NULL AND tag <> ''             -- filter out blanks
    GROUP BY country, tag, load_ts
),

country_summary AS (
    -- Step 3: Calculate total tag occurrences per country + load_ts
    -- Used to compute relative importance of tags later if needed.
    SELECT
        country,
        load_ts,
        COUNT(tag) AS total_tags
    FROM tag_counts
    GROUP BY country, load_ts
),

ranked_tags AS (
    -- Step 4: Rank tags within each country by frequency
    SELECT
        tc.country,
        tc.tag,
        tc.tag_count,
        cs.total_tags,
        tc.load_ts,
        ROW_NUMBER() OVER (
            PARTITION BY tc.country 
            ORDER BY tc.tag_count DESC
        ) AS rn
    FROM tag_counts tc
    JOIN country_summary cs
        ON tc.country = cs.country
       AND tc.load_ts = cs.load_ts
),

-- Step 5: Keep only the top 100 tags per country
top_100_tags as (
    SELECT
    country,
    tag,
    tag_count,
    total_tags,
    load_ts
FROM ranked_tags
WHERE rn <= 100
)

select top 10 * from top_100_tags order  by tag_count desc
