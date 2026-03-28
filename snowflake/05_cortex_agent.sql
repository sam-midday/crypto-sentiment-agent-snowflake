------------------------------------------------------------
-- 05_cortex_agent.sql
-- Cortex Agent combining Analyst + Search tools
------------------------------------------------------------

USE DATABASE CRYPTO_SENTIMENT_DB;
USE SCHEMA ANALYTICS;

-- Example: Query the Cortex Agent via SQL
-- This uses SNOWFLAKE.CORTEX.COMPLETE with tool_choice
-- to orchestrate Cortex Analyst (semantic view) + Cortex Search

SELECT SNOWFLAKE.CORTEX.COMPLETE(
  'claude-3-5-sonnet',
  CONCAT(
    'You are a crypto market and tech news analyst assistant. ',
    'You have access to two tools: ',
    '1) A Cortex Analyst tool that can query structured crypto price data (prices, market caps, volumes, daily changes) for Bitcoin, Ethereum, Solana, Cardano, and Dogecoin. ',
    '2) A Cortex Search tool that can search unstructured tech news articles from HackerNews with sentiment scores. ',
    'When answering, combine insights from both structured price data and unstructured news sentiment when relevant. ',
    'Always cite specific numbers and sources. ',
    'User question: What is the current state of the crypto market?'
  )
) AS agent_response;


-- ─── STREAMLIT USAGE (Python) ───────────────────────────────
-- In the Streamlit app, the agent is called via the Python API:
--
--   from snowflake.core import Root
--
--   root = Root(session)
--   agent = root.databases["CRYPTO_SENTIMENT_DB"].schemas["ANALYTICS"].cortex_agent("CRYPTO_AGENT")
--
--   response = agent.complete(
--       model="claude-3-5-sonnet",
--       tools=[
--           {"type": "cortex_analyst_tool", "semantic_view": "CRYPTO_SENTIMENT_DB.ANALYTICS.CRYPTO_PRICES_SEMANTIC_VIEW"},
--           {"type": "cortex_search_tool",  "search_service": "CRYPTO_SENTIMENT_DB.ANALYTICS.TECH_NEWS_SEARCH"},
--       ],
--       messages=[{"role": "user", "content": user_question}],
--   )

