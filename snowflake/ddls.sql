/*
YouTube Trending Data - Snowflake Setup & Access Configuration
--------------------------------------------------------------
This SQL script sets up the Snowflake environment required for storing,
processing, and analyzing YouTube Trending data.
*/

------------------------------------------------------------
-- 1. ROLE & USER PERMISSIONS
------------------------------------------------------------
CREATE ROLE YT_ROLE;
GRANT ROLE YT_ROLE TO USER YOUTUBE_USER;

------------------------------------------------------------
-- 2. WAREHOUSE SETUP (auto-suspend = 60 sec to save cost)
------------------------------------------------------------
CREATE WAREHOUSE YT_WH
  WITH WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE;

-- Allow YT_ROLE to use the warehouse
GRANT USAGE ON WAREHOUSE YT_WH TO ROLE YT_ROLE;

------------------------------------------------------------
-- 3. DATABASE & SCHEMA SETUP
------------------------------------------------------------
CREATE DATABASE YT_DB;
CREATE SCHEMA YT_DB.YOUTUBE;

-- Grant role access to database & schema
GRANT USAGE ON DATABASE YT_DB TO ROLE YT_ROLE;
GRANT USAGE ON SCHEMA YT_DB.YOUTUBE TO ROLE YT_ROLE;
GRANT CREATE TABLE, CREATE VIEW ON SCHEMA YT_DB.YOUTUBE TO ROLE YT_ROLE;

------------------------------------------------------------
-- 4. TABLES
------------------------------------------------------------

create or replace TABLE YT_DB.YOUTUBE.RAW_YOUTUBE_DATA (
	"video_id" VARCHAR(16777216),
	"published_at" VARCHAR(16777216),
	"channel_id" VARCHAR(16777216),
	"title" VARCHAR(16777216),
	"description" VARCHAR(16777216),
	"channel_title" VARCHAR(16777216),
	"category_id" VARCHAR(16777216),
	"live_broadcast_content" VARCHAR(16777216),
	"default_language" VARCHAR(16777216),
	"default_audio_language" VARCHAR(16777216),
	"tags" VARCHAR(16777216),
	"duration" VARCHAR(16777216),
	"dimension" VARCHAR(16777216),
	"definition" VARCHAR(16777216),
	"caption" VARCHAR(16777216),
	"licensed_content" BOOLEAN,
	"view_count" NUMBER(38,0),
	"like_count" FLOAT,
	"favorite_count" NUMBER(38,0),
	"comment_count" FLOAT,
	"blocked_countries" VARCHAR(16777216),
	"load_ts" NUMBER(38,0),
	"country" VARCHAR(16777216)
);


create or replace TABLE YT_DB.YOUTUBE.YOUTUBE_INCREMENTAL_LOAD (
	VIDEO_ID VARCHAR(16777216),
	PUBLISHED_AT VARCHAR(16777216),
	CHANNEL_ID VARCHAR(16777216),
	TITLE VARCHAR(16777216),
	DESCRIPTION VARCHAR(16777216),
	CHANNEL_TITLE VARCHAR(16777216),
	CATEGORY_ID VARCHAR(16777216),
	LIVE_BROADCAST_CONTENT VARCHAR(16777216),
	DEFAULT_LANGUAGE VARCHAR(16777216),
	DEFAULT_AUDIO_LANGUAGE VARCHAR(16777216),
	TAGS VARCHAR(16777216),
	DURATION NUMBER(38,0),
	DIMENSION VARCHAR(16777216),
	DEFINITION VARCHAR(16777216),
	CAPTION VARCHAR(16777216),
	LICENSED_CONTENT BOOLEAN,
	VIEW_COUNT NUMBER(38,0),
	LIKE_COUNT FLOAT,
	FAVORITE_COUNT NUMBER(38,0),
	COMMENT_COUNT FLOAT,
	BLOCKED_COUNTRIES VARCHAR(16777216),
	LOAD_TS VARCHAR(16777216),
	COUNTRY VARCHAR(16777216)
);

------------------------------------------------------------
-- 5. SECURITY (RSA key-based authentication)
------------------------------------------------------------
ALTER USER YOUTUBE_USER
SET RSA_PUBLIC_KEY='-----BEGIN PUBLIC KEY-----
XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
XXXXXXXXXXXXXXXXXXXXXXXX
-----END PUBLIC KEY-----';

------------------------------------------------------------
-- 6. SESSION MANAGEMENT
------------------------------------------------------------
ALTER SESSION SET TIMEZONE = 'Asia/Kolkata';
SHOW PARAMETERS LIKE 'TIMEZONE';

-- Switch context to YT_ROLE & warehouse
USE ROLE YT_ROLE;
USE WAREHOUSE YT_WH;
USE DATABASE YT_DB;
USE SCHEMA raw;

------------------------------------------------------------
-- 7. ANALYSIS QUERIES (Top 10 insights)
------------------------------------------------------------

-- Top Blocking Countries : Which countries have blocked the highest number of trending videos (Top 10)
SELECT * FROM mart_tt_blocking_countries;

-- Most-Blocked Videos : Which trending videos are blocked by the largest number of countries (Top 10)
SELECT * FROM mart_tt_blocked_videos;

-- Blocked Categories : Which video categories are blocked the most (Top 10)
SELECT * FROM mart_tt_blocked_category;

-- Most Affected Countries : Which countries trending videos are most frequently blocked (Top 10)
SELECT * FROM mart_tt_blocked_countries;

-- Popular Tags : Which tags appear most often in trending videos (Top 10)
SELECT * FROM mart_tt_popular_tags;

-- Highest-View Videos : Which trending videos have the highest view counts (Top 10)
SELECT * FROM mart_tt_viewed_videos;

-- Highest-Like Videos : Which trending videos have the highest like counts (Top 10)
SELECT * FROM mart_tt_liked_videos;

-- Highest-Comment Videos : Which trending videos have the highest comment counts (Top 10)
SELECT * FROM mart_tt_commented_videos;

-- Top Watching Countries : Which countries watch the most trending videos (Top 10)
SELECT * FROM mart_tt_watch_countries;

-- Highest Like-to-View % : Which trending videos have the highest engagement rate (likes per view) (Top 10)
SELECT * FROM mart_tt_like_view_percentage;

-- Highest Comment-to-View % : Which trending videos have the highest engagement rate (comments per view) (Top 10)
SELECT * FROM mart_tt_comment_view_percentage;

-- Highest Comment-to-Like % : Which trending videos have the highest engagement rate (comments per like) (Top 10)
SELECT * FROM mart_tt_comment_like_percentage;

-- Most Globally Trending Videos : Which trending videos appear in the highest number of countries (Top 10)
SELECT * FROM mart_tt_trending_videos;

-- Longest Trending Videos : Which trending videos have the longest durations (Top 10)
SELECT * FROM mart_tt_long_videos;

-- Shortest Trending Videos : Which trending videos have the shortest durations (Top 10)
SELECT * FROM mart_tt_short_videos;

-- Most Used Categories : Which categories appear most frequently in trending videos (Top 10)
SELECT * FROM mart_tt_most_used_category;

-- Lowest-View Categories : Which categories have the lowest average views across trending videos (Top 10)
SELECT * FROM mart_tt_least_viewed_category;

-- Highest-View Categories : Which categories have the highest total views (Top 10)
SELECT * FROM mart_tt_most_viewed_category;

-- Most Used Languages : Which languages are most common in trending videos (Top 10)
SELECT * FROM mart_tt_most_used_lang;

-- Most Viewed Languages : Which languages have the highest total views in trending videos (Top 10)
SELECT * FROM mart_tt_most_viewed_lang;

-- Trending Channel Leaders : Which channels appear most frequently in trending videos (Top 10)
SELECT * FROM mart_tt_trending_channel;

-- Highest-Like Channels : Which channels have the highest total likes (Top 10)
SELECT * FROM mart_tt_like_channel;

-- Highest-View Channels : Which channels have the highest total views (Top 10)
SELECT * FROM mart_tt_view_channel;

-- Highest-Comment Channels : Which channels have the highest total comments (Top 10)
SELECT * FROM mart_tt_comment_channel;
