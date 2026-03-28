------------------------------------------------------------
-- 03_semantic_view.sql
-- Semantic View over CRYPTO_PRICES for Cortex Analyst
------------------------------------------------------------

USE DATABASE CRYPTO_SENTIMENT_DB;
USE SCHEMA ANALYTICS;

CREATE OR REPLACE SEMANTIC VIEW CRYPTO_PRICES_SEMANTIC_VIEW
  AS SEMANTIC MODEL (
    ENTITIES (
      crypto_prices AS (
        SELECT *
        FROM CRYPTO_SENTIMENT_DB.ANALYTICS.CRYPTO_PRICES
      )
      PRIMARY KEY (COIN_ID, PRICE_DATE)

      ATTRIBUTES (
        COIN_ID
          COMMENT 'Unique identifier for each cryptocurrency (e.g. bitcoin, ethereum)'
          SYNONYMS ('coin', 'crypto', 'token', 'cryptocurrency'),
        COIN_NAME
          COMMENT 'Full name of the cryptocurrency'
          SYNONYMS ('name', 'crypto name'),
        SYMBOL
          COMMENT 'Ticker symbol (e.g. BTC, ETH, SOL)'
          SYNONYMS ('ticker', 'token symbol'),
        PRICE_DATE
          COMMENT 'Date of the price snapshot'
          SYNONYMS ('date', 'day', 'trading date')
      )

      MEASURES (
        CURRENT_PRICE
          COMMENT 'Current price of the cryptocurrency in USD'
          SYNONYMS ('price', 'value', 'cost')
          AGGREGATE BY SUM,
        MARKET_CAP
          COMMENT 'Total market capitalization in USD'
          SYNONYMS ('market cap', 'market value', 'capitalization')
          AGGREGATE BY SUM,
        TOTAL_VOLUME
          COMMENT '24-hour trading volume in USD'
          SYNONYMS ('volume', 'trading volume', '24h volume')
          AGGREGATE BY SUM,
        PRICE_CHANGE_PCT_24H
          COMMENT 'Price change percentage over last 24 hours'
          SYNONYMS ('price change', 'change', '24h change', 'daily change')
          AGGREGATE BY AVG,
        HIGH_24H
          COMMENT '24-hour high price in USD'
          SYNONYMS ('high', 'daily high', '24h high')
          AGGREGATE BY MAX,
        LOW_24H
          COMMENT '24-hour low price in USD'
          SYNONYMS ('low', 'daily low', '24h low')
          AGGREGATE BY MIN
      )
    )

    FACTS ()
  );

COMMENT ON SEMANTIC VIEW CRYPTO_PRICES_SEMANTIC_VIEW IS
  'Semantic model for cryptocurrency price data. Supports questions about prices, market caps, volumes, and daily changes across Bitcoin, Ethereum, Solana, Cardano, and Dogecoin.';

