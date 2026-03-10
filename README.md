# Netflix Cultural Intelligence Platform

A Streamlit-in-Snowflake application for analyzing cultural trends across TikTok, Reddit, and YouTube using **SocialGist AI-Ready Social Data** and **Snowflake Cortex AI SQL Functions**.

## What's Included

| File | Description |
|---|---|
| `streamlit_app_aisql.py` | Main Streamlit application (10-tab cultural intelligence platform) |
| `environment.yml` | Python dependencies for Streamlit-in-Snowflake |
| `setup.sql` | SQL deployment script |

## Prerequisites

1. **SocialGist AI-Ready Social Data** listing installed in your Snowflake account
   - This creates the database: `NETFLIX__ROMANTASY_AIREADY_SOCIAL_DATA`
   - Schema: `NETFLIX_TRIAL`
   - Includes: 10 data views, 2 Cortex Search Services, 1 Semantic View
2. **A Snowflake warehouse** (e.g., `COMPUTE_WH`)
3. **A role** with:
   - `CREATE DATABASE` / `CREATE SCHEMA` / `CREATE STAGE` / `CREATE STREAMLIT` privileges (or use an existing database/schema you own)
   - `USAGE` on the SocialGist database
   - `SNOWFLAKE.CORTEX_USER` database role (for AI SQL functions and Cortex Analyst)

## Quick Deploy (Snowsight UI -- No SQL Required)

1. Open **Snowsight** and go to **Projects > Streamlit**
2. Click **+ Streamlit App**
3. Choose a database, schema, and warehouse
4. Paste the contents of `streamlit_app_aisql.py` into the code editor
5. In the left sidebar, open the `environment.yml` file and replace its contents with:
   ```yaml
   name: sf_env
   channels:
     - snowflake
   dependencies:
     - streamlit
     - altair
     - snowflake.core
   ```
   > **Note:** Do **not** add `snowflake` (the meta-package) or `snowflake-snowpark-python` — they cause package resolution conflicts.
6. Click **Run**

## Deploy via SQL

### Step 1: Edit `setup.sql`

Open `setup.sql` and update the warehouse name on this line:

```sql
SET warehouse_name = 'COMPUTE_WH';  -- << Change to your warehouse
```

### Step 2: Upload Files and Run Setup

**Option A: Using Snowsight**

1. Open a SQL Worksheet in Snowsight
2. Run the contents of `setup.sql` (it creates the database, schema, and stage)
3. Upload `streamlit_app_aisql.py` and `environment.yml` to the stage:
   - Navigate to **Data > Databases > NETFLIX_CULTURAL_INTELLIGENCE > APP > Stages > APP_STAGE**
   - Click **+ Files** and upload both files
4. Run the remaining `CREATE STREAMLIT` and `GRANT` statements from `setup.sql`

**Option B: Using SnowSQL CLI**

```bash
# Connect to your account
snowsql -a <your_account> -u <your_user>

# Run the setup script
!source setup.sql

# Upload files
PUT file://streamlit_app_aisql.py @NETFLIX_CULTURAL_INTELLIGENCE.APP.APP_STAGE AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
PUT file://environment.yml @NETFLIX_CULTURAL_INTELLIGENCE.APP.APP_STAGE AUTO_COMPRESS=FALSE OVERWRITE=TRUE;

# Create the Streamlit app (copy from setup.sql)
CREATE OR REPLACE STREAMLIT NETFLIX_CULTURAL_INTELLIGENCE.APP.CULTURAL_INTELLIGENCE_AISQL
  ROOT_LOCATION = '@NETFLIX_CULTURAL_INTELLIGENCE.APP.APP_STAGE'
  MAIN_FILE = 'streamlit_app_aisql.py'
  QUERY_WAREHOUSE = '<YOUR_WAREHOUSE>'
  TITLE = 'Netflix Cultural Intelligence (AI SQL)';

GRANT USAGE ON STREAMLIT NETFLIX_CULTURAL_INTELLIGENCE.APP.CULTURAL_INTELLIGENCE_AISQL TO ROLE PUBLIC;
```

### Step 3: Open the App

Navigate to **Projects > Streamlit** in Snowsight and click on **Netflix Cultural Intelligence (AI SQL)**.

## App Features (10 Tabs)

| Tab | Description | Snowflake Feature |
|---|---|---|
| Pulse Dashboard | KPIs, platform breakdown, daily volume | AI_CLASSIFY, AI_SENTIMENT |
| AI Search & Chat | Semantic search + AI summarization | Cortex Search, AI_COMPLETE |
| Sentiment & Emotion | Sentiment analysis with emotion detection | AI_SENTIMENT, AI_CLASSIFY |
| Conversation Explorer | Browse posts, TikTok analytics, threads | AI_FILTER, AI_COMPLETE |
| Audience Intelligence | Creator profiling and audience segmentation | AI_CLASSIFY, AI_COMPLETE |
| Narrative Lens | Cultural theme detection and analysis | AI_CLASSIFY, AI_COMPLETE |
| Cross-Platform Comparison | Platform-by-platform metrics comparison | AI_SUMMARIZE_AGG |
| Deep Dive Reports | AI-generated long-form analysis | AI_COMPLETE, AI_AGG |
| Trend Radar | Week-over-week trending topics | AI_CLASSIFY |
| Cortex Analyst | Natural language to SQL queries | Cortex Analyst API + Semantic View |

## Data Source

All data comes from the **SocialGist AI-Ready Social Data** Snowflake Marketplace listing:

- **Database**: `NETFLIX__ROMANTASY_AIREADY_SOCIAL_DATA`
- **Schema**: `NETFLIX_TRIAL`
- **Tables/Views**: AUTHORS, CONTENT_ITEMS, CONTENT_TEXT, CONTENT_CHUNKS, CONTENT_RELATIONSHIPS, CONTENT_TAG, TAGS, REDDIT_AUTHORS, TIKTOK_POSTS, TIKTOK_CREATORS
- **Cortex Search Services**: TEXT_CHUNK_SEARCH, AUTHOR_DESCRIPTION_SEARCH
- **Semantic View**: SOCIAL_CONVERSATIONS

## Troubleshooting

| Issue | Fix |
|---|---|
| `Cannot create a Python function with the specified packages` | Remove `snowflake`, `snowflake-snowpark-python`, and `streamlit` from your packages/environment.yml — they are bundled in the SiS runtime and cause version conflicts. Only `altair` and `snowflake.core` are needed. |
| `Object does not exist: NETFLIX__ROMANTASY_AIREADY_SOCIAL_DATA` | Install the SocialGist listing from Snowflake Marketplace |
| `Cortex Search Service not found` | The search services are created by the SocialGist listing -- ensure the listing is fully installed |
| `AI_COMPLETE / AI_SENTIMENT not recognized` | Ensure your account has Cortex AI SQL Functions enabled and your role has SNOWFLAKE.CORTEX_USER |
| `Cortex Analyst returns HTTP 403` | Grant SNOWFLAKE.CORTEX_USER or SNOWFLAKE.CORTEX_ANALYST_USER database role to your role |
| Streamlit app is slow to load | First load initializes the environment; subsequent loads are faster |
