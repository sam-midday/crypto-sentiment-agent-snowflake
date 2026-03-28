------------------------------------------------------------
-- 02_tables.sql
-- Create the two core tables
------------------------------------------------------------

USE DATABASE CRYPTO_SENTIMENT_DB;
USE SCHEMA ANALYTICS;

CREATE TABLE IF NOT EXISTS CRYPTO_PRICES (
    COIN_ID                 VARCHAR(50)    NOT NULL,
    COIN_NAME               VARCHAR(100),
    SYMBOL                  VARCHAR(10),
    PRICE_DATE              DATE           NOT NULL,
    CURRENT_PRICE           FLOAT,
    MARKET_CAP              FLOAT,
    TOTAL_VOLUME            FLOAT,
    PRICE_CHANGE_PCT_24H    FLOAT,
    HIGH_24H                FLOAT,
    LOW_24H                 FLOAT,
    LOADED_AT               TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT PK_CRYPTO_PRICES PRIMARY KEY (COIN_ID, PRICE_DATE)
);

CREATE TABLE IF NOT EXISTS TECH_NEWS (
    STORY_ID        VARCHAR(20)   NOT NULL,
    TITLE           VARCHAR(500),
    URL             VARCHAR(2000),
    AUTHOR          VARCHAR(200),
    POINTS          INT,
    NUM_COMMENTS    INT,
    CREATED_AT      TIMESTAMP_NTZ,
    LOAD_DATE       DATE,
    CONTENT         VARCHAR(5000),
    SEARCH_TERM     VARCHAR(50),
    SENTIMENT_SCORE FLOAT,
    SENTIMENT_LABEL VARCHAR(20),
    LOADED_AT       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT PK_TECH_NEWS PRIMARY KEY (STORY_ID)
);

