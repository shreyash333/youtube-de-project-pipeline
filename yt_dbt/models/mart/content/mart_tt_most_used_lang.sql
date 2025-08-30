{{ config(materialized='table') }}

-- Model: mart_tt_most_used_lang
-- Most Used Languages : Which languages are most common in trending videos (Top 10)
-- Aggregates trending videos by their default_audio_language.
-- Counts how many unique videos exist for each language,
-- and how many unique countries those videos appeared in.

with ranked_videos as (
select
    default_audio_language,                -- Language of the video audio track
    count(distinct video_id) as videos_count,   -- Number of unique videos in this language
    count(distinct country) as country_count    -- Number of unique countries where those videos trended
from {{ref('int_unique_video')}}
 where default_audio_language is not null -- Consider only videos with an audio language
group by default_audio_language
)

select top 10 * from ranked_videos order by  videos_count desc, country_count desc