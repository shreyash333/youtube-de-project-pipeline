{{
    config(
        materialized = 'incremental',           
        unique_key = ['video_id', 'load_ts'],
        transient = false    
    )
}}

-- Model: youtube_incremental_load
-- Description:
--   This model incrementally loads YouTube trending data from the staging table.
--   Each row is uniquely identified by (video_id, load_ts), ensuring that:
--     - Historical snapshots of the same video across different load timestamps are preserved.
--     - Duplicate loads for the same video_id + load_ts are avoided.
-- Logic:
--   On initial run → loads all rows.
--   On subsequent runs → only loads rows that don’t already exist in the target table.

SELECT
    video_id,                -- unique identifier of the video
    published_at,            -- when the video was originally published
    channel_id,              -- unique identifier of the channel
    title,                   -- video title
    description,             -- video description text
    channel_title,           -- name of the channel
    category_id,             -- YouTube category identifier
    live_broadcast_content,  -- whether the video is live, none, or upcoming
    default_language,        -- default metadata language
    default_audio_language,  -- spoken audio language of the video
    tags,                    -- list of tags for the video
    duration,                -- length of the video
    dimension,               -- video dimension (2D/3D)
    definition,              -- HD/SD definition
    caption,                 -- whether captions are available
    licensed_content,        -- whether it’s licensed content
    view_count,              -- number of views
    like_count,              -- number of likes
    favorite_count,          -- legacy field (always 0 in API v3)
    comment_count,           -- number of comments
    blocked_countries,       -- list of countries where video is blocked
    load_ts,                 -- timestamp of when data was ingested
    country                  -- country where video appeared in trending
FROM {{ ref('stg_youtube_data') }}

{% if is_incremental() %}
    -- Only insert new rows that don’t already exist in the target table.
    -- This prevents duplicates while keeping history across load_ts snapshots.
    WHERE (video_id, load_ts) NOT IN (
        SELECT video_id, load_ts
        FROM {{ this }}
    )
{% endif %}
