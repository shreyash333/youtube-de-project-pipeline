"""
YouTube Trending Reports - Snowflake to PDF Generator
------------------------------------------------------

This script connects to Snowflake using RSA key-based authentication, runs multiple
analytical queries on YouTube trending video data, and generates a structured PDF report.

Key Responsibilities:
1. Authenticate and connect to Snowflake using private RSA key.
2. Execute predefined queries for different analytical perspectives:
   - Top blocking countries
   - Most-blocked videos
   - Popular tags
   - Highest-viewed videos
   - Engagement metrics (likes/views, comments/views, etc.)
   - Top-performing categories, languages, and channels
3. Fetch query results into Pandas DataFrames.
4. Format results into tables with styled headers.
5. Generate a single consolidated PDF report with sections for each analysis.

Output:
- A PDF file named `youtube_trending_report_<YYYY_MM_DD>.pdf` 
  containing all analysis tables.

"""

import pandas as pd
import snowflake.connector
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet
from snowflake.connector.pandas_tools import write_pandas
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from datetime import datetime
import os
import configparser

config = configparser.ConfigParser()
config.read("config.cfg")

# ------------------------------------------------------------------------------
# Step 1: Setup - Generate today's date for file naming
# ------------------------------------------------------------------------------
today = datetime.today().strftime('%Y_%m_%d')

# ------------------------------------------------------------------------------
# Step 2: Load Snowflake RSA Private Key
# ------------------------------------------------------------------------------
with open(".ssh/snowflake_rsa_key.p8", "rb") as key_file:
    private_key = serialization.load_pem_private_key(
        key_file.read(),
        password=None,
        backend=default_backend()
    )

# ------------------------------------------------------------------------------
# Step 3: Connect to Snowflake
# ------------------------------------------------------------------------------
conn = snowflake.connector.connect(
    user= config.get("SNOWFLAKE", "user"),                    # Your Snowflake username
    account= config.get("SNOWFLAKE", "account"),              # Snowflake account identifier
    private_key=private_key,                                  # Loaded RSA private key
    role= config.get("SNOWFLAKE", "role"),                    # Role with access
    warehouse= config.get("SNOWFLAKE", "warehouse"),          # Compute warehouse
    database= config.get("SNOWFLAKE", "database"),            # Target database
    schema= config.get("SNOWFLAKE", "schema")                 # Target schema
)

# ------------------------------------------------------------------------------
# Step 4: Define Analytical Queries
# Each query generates a different view of YouTube trending video performance
# ------------------------------------------------------------------------------
queries = {
    'Top Blocking Countries : Which countries have blocked the highest number of trending videos (Top 10)' : """
        select * from mart_tt_blocking_countries;
    """,
    'Most-Blocked Videos : Which trending videos are blocked by the largest number of countries (Top 10)' : """
        select * from mart_tt_blocked_videos;
    """,
    'Blocked Categories : Which video categories are blocked the most (Top 10)' : """
        select * from mart_tt_blocked_category;
    """,
    'Most Affected Countries : Which countries trending videos are most frequently blocked (Top 10)' : """
        select * from mart_tt_blocked_countries;
    """,
    'Popular Tags : Which tags appear most often in trending videos (Top 10)' : """
        select * from mart_tt_popular_tags;
    """,
    'Highest-View Videos : Which trending videos have the highest view counts (Top 10)' : """
        select * from mart_tt_viewed_videos;
    """,
    'Highest-Like Videos : Which trending videos have the highest like counts (Top 10)' : """
        select * from mart_tt_liked_videos;
    """,
    'Highest-Comment Videos : Which trending videos have the highest comment counts (Top 10)' : """
        select * from mart_tt_commented_videos;
    """,
    'Top Watching Countries : Which countries watch the most trending videos (Top 10)' : """
        select * from mart_tt_watch_countries;
    """,
    'Highest Like-to-View % : Which trending videos have the highest engagement rate (likes per view) (Top 10)' : """
        select * from mart_tt_like_view_percentage;
    """,
    'Highest Comment-to-View % : Which trending videos have the highest engagement rate (comments per view) (Top 10)' : """
        select * from mart_tt_comment_view_percentage;
    """,
    'Highest Comment-to-Like % : Which trending videos have the highest engagement rate (comments per like) (Top 10)' : """
        select * from mart_tt_comment_like_percentage;
    """,
    'Most Globally Trending Videos : Which trending videos appear in the highest number of countries (Top 10)' : """
        select * from mart_tt_trending_videos;
    """,
    'Longest Trending Videos : Which trending videos have the longest durations (Top 10)' : """
        select * from mart_tt_long_videos;
    """,
    'Shortest Trending Videos : Which trending videos have the shortest durations (Top 10)' : """
        select * from mart_tt_short_videos;
    """,
    'Most Used Categories : Which categories appear most frequently in trending videos (Top 10)' : """
        select * from mart_tt_most_used_category;
    """,
    'Lowest-View Categories : Which categories have the lowest average views across trending videos (Top 10)' : """
        select * from mart_tt_least_viewed_category;
    """,
    'Highest-View Categories : Which categories have the highest total views (Top 10)' : """
        select * from mart_tt_most_viewed_category;
    """,
    'Most Used Languages : Which languages are most common in trending videos (Top 10)' : """
        select * from mart_tt_most_used_lang;
    """,
    'Most Viewed Languages : Which languages have the highest total views in trending videos (Top 10)' : """
        select * from mart_tt_most_viewed_lang;
    """,
    'Trending Channel Leaders : Which channels appear most frequently in trending videos (Top 10)' : """
        select * from mart_tt_trending_channel;
    """,
    'Highest-Like Channels : Which channels have the highest total likes (Top 10)' : """
        select * from mart_tt_like_channel;
    """,
    'Highest-View Channels : Which channels have the highest total views (Top 10)' : """
        select * from mart_tt_view_channel;
    """,
    'Highest-Comment Channels : Which channels have the highest total comments (Top 10)' : """
        select * from mart_tt_comment_channel;
    """
}

# ------------------------------------------------------------------------------
# Step 5: Setup PDF Report
# ------------------------------------------------------------------------------
from reportlab.lib.pagesizes import A2
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.cidfonts import UnicodeCIDFont
import pandas as pd
import datetime

# Register wide-coverage Unicode CID font (covers multiple scripts)
pdfmetrics.registerFont(UnicodeCIDFont("STSong-Light"))  

today = datetime.date.today().strftime("%Y-%m-%d")
pdf_file = f"youtube_trending_report_{today}.pdf"
doc = SimpleDocTemplate(pdf_file, pagesize=A2)
elements = []
styles = getSampleStyleSheet()

# Create custom styles
styles.add(ParagraphStyle(name="Heading1_Custom", parent=styles["Heading1"], fontName="Times-Roman"))
styles.add(ParagraphStyle(name="Heading2_Custom", parent=styles["Heading2"], fontName="Times-Roman"))
styles.add(ParagraphStyle(name="BodyUnicode", parent=styles["BodyText"], fontName="STSong-Light"))

# Title
elements.append(Paragraph('YouTube Trending Videos Data Analysis', styles['Heading1_Custom']))
elements.append(Spacer(1, 12))

# ------------------------------------------------------------------------------
# Step 6: Loop Through Queries, Fetch Data, Add to Report
# ------------------------------------------------------------------------------
for title, query in queries.items():
    df = pd.read_sql(query, conn)

    # Section title (Times font)
    elements.append(Paragraph(title, styles['Heading2_Custom']))
    elements.append(Spacer(1, 6))

    # Convert DataFrame to table
    table_data = [df.columns.tolist()] + df.values.tolist()
    table = Table(table_data, hAlign="LEFT")

    # Apply styling to table (text with Unicode font)
    table.setStyle(TableStyle([
        ("BACKGROUND", (0,0), (-1,0), colors.grey),
        ("TEXTCOLOR", (0,0), (-1,0), colors.whitesmoke),
        ("ALIGN", (0,0), (-1,-1), "LEFT"),
        ("GRID", (0,0), (-1,-1), 0.5, colors.black),
        ("FONTSIZE", (0,0), (-1,-1), 8),
        ("FONTNAME", (0,0), (-1,-1), "STSong-Light"),  # ensures unicode text shows correctly
    ]))

    elements.append(table)
    elements.append(Spacer(1, 12))

# ------------------------------------------------------------------------------
# Step 7: Build PDF
# ------------------------------------------------------------------------------
doc.build(elements)
print(f"Report generated: {pdf_file}")
