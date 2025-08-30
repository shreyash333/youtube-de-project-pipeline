-- models/staging/int_block_country.sql
{{ config(
    materialized='view'
) }}

-- Model: int_block_country
-- Description:
--   This model takes the `blocked_countries` column (a comma-separated list of country codes)
--   from the raw `blocked_videos` table and normalizes it into one row per country per video.
--   This makes it easier to filter and analyze blocked content by specific countries.
--
--   Key steps:
--   - Reads raw blocked video data from the `yt_db.raw.blocked_videos` table.
--   - Uses `SPLIT_TO_TABLE` to break apart the comma-separated list into rows.
--   - Cleans up the values (trimming whitespace, filtering out null/empty strings).
--   - Outputs a clean dataset with `video_id` and individual `country` rows.

with exploded_countries as (
    select
        *,
        trim(value) as block_by_country
    from {{ref('int_block_video')}},
         lateral split_to_table(blocked_countries, ',')
    where blocked_countries is not null
      and blocked_countries <> ''
)

select *
from exploded_countries
