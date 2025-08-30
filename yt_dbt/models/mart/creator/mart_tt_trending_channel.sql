{{ config(materialized='table') }}

-- Model: mart_tt_trending_channel
-- Trending Channel Leaders : Which channels appear most frequently in trending videos (Top 10)
-- Aggregates trending data at the channel level
-- to show how many unique trending videos a channel has,
-- and across how many countries those videos appeared.

with trending_videos as (
    select
    channel_title,                                -- Channel name
    count(distinct video_id) as trending_videos_count, -- Number of unique trending videos for the channel
    count(*) as countries_count                   -- Number of records (country occurrences) → shows how many times videos trended across countries
from {{ ref('stg_youtube_data') }}
group by channel_id, channel_title
)

select top 10 * from trending_videos order by countries_count desc, trending_videos_count desc