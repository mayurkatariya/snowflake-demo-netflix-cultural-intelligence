/*
 * Netflix Cultural Intelligence Platform — Deployment Script
 * 
 * Prerequisites:
 *   1. SocialGist AI-Ready Social Data listing installed
 *      (database: NETFLIX__ROMANTASY_AIREADY_SOCIAL_DATA)
 *   2. Update the warehouse name below
 *
 * Usage:
 *   1. Run this script in a Snowsight SQL Worksheet or SnowSQL
 *   2. Upload streamlit_app_aisql.py and environment.yml to the stage
 *   3. Run the CREATE STREAMLIT statement at the bottom
 */

-- ============================================================================
-- CONFIGURATION — Update this to match your environment
-- ============================================================================
SET warehouse_name = 'COMPUTE_WH';  -- << CHANGE THIS to your warehouse name

-- ============================================================================
-- CREATE DATABASE, SCHEMA, AND STAGE
-- ============================================================================
CREATE DATABASE IF NOT EXISTS NETFLIX_CULTURAL_INTELLIGENCE;
CREATE SCHEMA IF NOT EXISTS NETFLIX_CULTURAL_INTELLIGENCE.APP;

CREATE STAGE IF NOT EXISTS NETFLIX_CULTURAL_INTELLIGENCE.APP.APP_STAGE
  DIRECTORY = (ENABLE = TRUE);

-- ============================================================================
-- UPLOAD FILES
-- Run these from SnowSQL (not Snowsight — Snowsight uses the UI file uploader)
-- ============================================================================
-- PUT file://streamlit_app_aisql.py @NETFLIX_CULTURAL_INTELLIGENCE.APP.APP_STAGE AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
-- PUT file://environment.yml @NETFLIX_CULTURAL_INTELLIGENCE.APP.APP_STAGE AUTO_COMPRESS=FALSE OVERWRITE=TRUE;

-- ============================================================================
-- VERIFY PREREQUISITES
-- ============================================================================

-- Check SocialGist data is accessible
SELECT COUNT(*) AS content_items_count
FROM NETFLIX__ROMANTASY_AIREADY_SOCIAL_DATA.NETFLIX_TRIAL.CONTENT_ITEMS
LIMIT 1;

-- Check Cortex Search Services exist
SHOW CORTEX SEARCH SERVICES IN SCHEMA NETFLIX__ROMANTASY_AIREADY_SOCIAL_DATA.NETFLIX_TRIAL;

-- Check Semantic View exists
SHOW SEMANTIC VIEWS IN SCHEMA NETFLIX__ROMANTASY_AIREADY_SOCIAL_DATA.NETFLIX_TRIAL;

-- ============================================================================
-- CREATE THE STREAMLIT APP
-- Run this AFTER uploading the files to the stage
-- ============================================================================
CREATE OR REPLACE STREAMLIT NETFLIX_CULTURAL_INTELLIGENCE.APP.CULTURAL_INTELLIGENCE_AISQL
  ROOT_LOCATION = '@NETFLIX_CULTURAL_INTELLIGENCE.APP.APP_STAGE'
  MAIN_FILE = 'streamlit_app_aisql.py'
  QUERY_WAREHOUSE = IDENTIFIER($warehouse_name)
  TITLE = 'Netflix Cultural Intelligence (AI SQL)';

-- ============================================================================
-- GRANT ACCESS
-- Update the role name to match who should have access
-- ============================================================================
GRANT USAGE ON DATABASE NETFLIX_CULTURAL_INTELLIGENCE TO ROLE PUBLIC;
GRANT USAGE ON SCHEMA NETFLIX_CULTURAL_INTELLIGENCE.APP TO ROLE PUBLIC;
GRANT USAGE ON STREAMLIT NETFLIX_CULTURAL_INTELLIGENCE.APP.CULTURAL_INTELLIGENCE_AISQL TO ROLE PUBLIC;

-- ============================================================================
-- DONE — Open the app in Snowsight: Projects > Streamlit
-- ============================================================================
SELECT 'Deployment complete! Open Snowsight > Projects > Streamlit to launch the app.' AS status;
