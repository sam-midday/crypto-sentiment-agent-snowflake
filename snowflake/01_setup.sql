------------------------------------------------------------
-- 01_setup.sql
-- Create warehouse, database, and schema
------------------------------------------------------------

USE ROLE SYSADMIN;

CREATE WAREHOUSE IF NOT EXISTS CRYPTO_WH
  WAREHOUSE_SIZE = 'XSMALL'
  AUTO_SUSPEND   = 60
  AUTO_RESUME    = TRUE;

CREATE DATABASE IF NOT EXISTS CRYPTO_SENTIMENT_DB;
CREATE SCHEMA IF NOT EXISTS CRYPTO_SENTIMENT_DB.ANALYTICS;

USE WAREHOUSE CRYPTO_WH;
USE DATABASE CRYPTO_SENTIMENT_DB;
USE SCHEMA ANALYTICS;

