{{ config(materialized='table') }}

-- Model: mart_tt_most_viewed_lang
-- Most Viewed Languages : Which languages have the highest total views in trending videos (Top 10)
-- Aggregates trending videos by their default_audio_language.
-- Sum up all view_count per language and sort it 

with ranked_videos as (
select
    default_audio_language,                -- Language of the video audio track
    count(distinct video_id) as videos_count,   -- Number of unique videos in this language
    sum(view_count) as views_count,
     count(distinct country) as country_count    -- Number of unique countries where videos appeared
from {{ref('int_unique_video')}}
 where default_audio_language is not null -- Consider only videos with an audio language
group by default_audio_language
)

select top 10 * from ranked_videos order by  views_count desc