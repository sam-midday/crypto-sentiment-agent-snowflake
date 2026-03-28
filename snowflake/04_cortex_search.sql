------------------------------------------------------------
-- 04_cortex_search.sql
-- Cortex Search Service over TECH_NEWS
------------------------------------------------------------

USE DATABASE CRYPTO_SENTIMENT_DB;
USE SCHEMA ANALYTICS;

CREATE OR REPLACE CORTEX SEARCH SERVICE TECH_NEWS_SEARCH
  ON CONTENT
  ATTRIBUTES TITLE, AUTHOR, SEARCH_TERM, SENTIMENT_LABEL
  WAREHOUSE = CRYPTO_WH
  TARGET_LAG = '1 hour'
  AS (
    SELECT
      STORY_ID,
      TITLE,
      CONTENT,
      AUTHOR,
      SEARCH_TERM,
      SENTIMENT_LABEL,
      POINTS,
      NUM_COMMENTS,
      CREATED_AT
    FROM CRYPTO_SENTIMENT_DB.ANALYTICS.TECH_NEWS
  );

