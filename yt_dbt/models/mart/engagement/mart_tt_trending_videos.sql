{{ config(materialized='table') }}

-- Model: mart_tt_trending_videos
-- Most Globally Trending Videos : Which trending videos appear in the highest number of countries (Top 10)
-- Identifies the top 10 videos that trended in the most countries.
-- Then enriches them with detailed metadata (one row per video, highest view_count).

with most_trending_video as (

    -- Step 1: Count in how many countries each video has trended
    select 
        video_id,
        count(distinct country) as country_count
    from {{ ref('stg_youtube_data') }}
    group by video_id
    order by country_count desc
    limit 500   -- Keep top 500 videos across most countries
),

ranked_videos as (

    -- Step 2: Rank rows by view_count for each video
    select
        yt.*,
        row_number() over (
            partition by video_id
            order by view_count desc
        ) as rn
    from {{ ref('stg_youtube_data') }} yt
),

video_details as (

    -- Step 3: Select video metadata + join with top 500 list
    select
        title,
        published_at,
        channel_title,
        category_id,
        view_count,
        tv.country_count
    from ranked_videos rv
    join most_trending_video tv 
        on rv.video_id = tv.video_id
    where rn = 1   -- Only the highest view_count row per video
)

-- Step 4: Final output ordered by reach (country_count)
select top 10 * from video_details order by country_count desc
