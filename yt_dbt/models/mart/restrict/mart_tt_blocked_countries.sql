{{ config(materialized='table') }}

-- Model: mart_tt_blocked_countries
-- Description:
-- Most Affected Countries : Which countries trending videos are most frequently blocked (Top 10)
-- For each source country, this model calculates:
-- 1) the number of distinct trending videos from that country that are blocked elsewhere
-- 2) the number of distinct blocking countries involved.
-- It explodes the comma-separated blocked_countries, then aggregates by country.

with country_block AS (
    SELECT
        country,
        COUNT(DISTINCT block_by_country) AS blocked_country_count
    FROM {{ ref ('int_block_country')}}
    GROUP BY country
),

video_block AS (
    SELECT
        country,
        COUNT(DISTINCT video_id) AS blocked_video_count
    FROM {{ ref ('int_block_country')}}
    GROUP BY country
),

final_block as (
    SELECT
    cb.country,
    vb.blocked_video_count,
    cb.blocked_country_count
FROM country_block cb
JOIN video_block vb
  ON cb.country = vb.country
)

select top 10 * from final_block ORDER BY blocked_video_count DESC, blocked_country_count DESC
