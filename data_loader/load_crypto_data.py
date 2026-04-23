"""
Crypto Sentiment Agent - Data Loader
Fetches live crypto prices from CoinGecko and tech news from HackerNews,
then loads them into Snowflake tables.

Run: python load_crypto_data.py
Schedule with cron (Linux/Mac) or Task Scheduler (Windows) for daily loads.
"""

import requests
import snowflake.connector
from datetime import datetime, timezone

# ─── CONFIG ──────────────────────────────────────────────────
# Update these with your Snowflake credentials
SNOWFLAKE_CONFIG = {
    "account":   "<account-identifier>",      # e.g. "abc12345.us-east-1"
    "user":      "username",
    "password":  "password",
    "warehouse": "WAREHOUSE",
    "database":  "DATABASE",
    "schema":    "SCHEMA",
}

COINS = ["bitcoin", "ethereum", "solana", "cardano", "dogecoin"]
HN_SEARCH_TERMS = ["crypto", "bitcoin", "ethereum", "blockchain", "defi", "AI"]
# ─────────────────────────────────────────────────────────────


def fetch_crypto_prices():
    """Fetch current prices from CoinGecko (free, no API key needed)."""
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {
        "vs_currency": "usd",
        "ids": ",".join(COINS),
        "order": "market_cap_desc",
        "per_page": len(COINS),
        "page": 1,
        "sparkline": "false",
        "price_change_percentage": "24h",
    }
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    rows = []
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    for coin in data:
        rows.append((
            coin["id"],
            coin["name"],
            coin["symbol"].upper(),
            today,
            coin.get("current_price"),
            coin.get("market_cap"),
            coin.get("total_volume"),
            coin.get("price_change_percentage_24h"),
        ))
    print(f"Fetched prices for {len(rows)} coins")
    return rows


def fetch_hackernews_stories():
    """Fetch top stories from HackerNews (completely free, no auth)."""
    top_url = "https://hacker-news.firebaseio.com/v0/topstories.json"
    resp = requests.get(top_url, timeout=30)
    resp.raise_for_status()
    story_ids = resp.json()[:100]

    rows = []
    for sid in story_ids:
        try:
            detail = requests.get(
                f"https://hacker-news.firebaseio.com/v0/item/{sid}.json",
                timeout=10
            ).json()
            if not detail or detail.get("type") != "story":
                continue

            title = (detail.get("title") or "").lower()
            text = detail.get("text") or detail.get("title") or ""

            is_relevant = any(term in title for term in
                             ["crypto", "bitcoin", "btc", "ethereum", "eth",
                              "blockchain", "defi", "solana", "cardano",
                              "dogecoin", "nft", "web3", "ai ", "openai",
                              "llm", "gpu", "nvidia", "machine learning"])
            if not is_relevant:
                continue

            category = "crypto"
            if any(t in title for t in ["ai ", "openai", "llm", "gpu",
                                         "nvidia", "machine learning"]):
                category = "AI"
            elif any(t in title for t in ["python", "rust", "linux",
                                           "github", "programming"]):
                category = "tech"

            published = datetime.fromtimestamp(
                detail.get("time", 0), tz=timezone.utc
            ).strftime("%Y-%m-%d %H:%M:%S")

            rows.append((
                detail["id"],
                detail.get("title", "")[:1000],
                detail.get("url", "")[:2000],
                detail.get("by", "unknown")[:200],
                detail.get("score", 0),
                detail.get("descendants", 0),
                text[:16000],
                published,
                category,
            ))
        except Exception as e:
            print(f"  Skipping story {sid}: {e}")
            continue

    print(f"Fetched {len(rows)} relevant stories from HackerNews")
    return rows


def load_to_snowflake(price_rows, news_rows):
    """Load data into Snowflake tables using MERGE (upsert) to avoid duplicates."""
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    cur = conn.cursor()

    try:
        # ── Load crypto prices (MERGE = insert or update) ──
        if price_rows:
            cur.execute("""
                CREATE OR REPLACE TEMPORARY TABLE CRYPTO_SENTIMENT_DB.ANALYTICS.PRICES_STAGING (
                    COIN_ID VARCHAR, COIN_NAME VARCHAR, SYMBOL VARCHAR,
                    PRICE_DATE DATE, PRICE_USD FLOAT, MARKET_CAP_USD FLOAT,
                    VOLUME_USD FLOAT, PRICE_CHANGE_PCT_24H FLOAT
                )
            """)
            cur.executemany(
                "INSERT INTO CRYPTO_SENTIMENT_DB.ANALYTICS.PRICES_STAGING VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
                price_rows
            )
            cur.execute("""
                MERGE INTO CRYPTO_SENTIMENT_DB.ANALYTICS.CRYPTO_PRICES t
                USING CRYPTO_SENTIMENT_DB.ANALYTICS.PRICES_STAGING s
                ON t.COIN_ID = s.COIN_ID AND t.PRICE_DATE = s.PRICE_DATE
                WHEN MATCHED THEN UPDATE SET
                    t.PRICE_USD = s.PRICE_USD, t.MARKET_CAP_USD = s.MARKET_CAP_USD,
                    t.VOLUME_USD = s.VOLUME_USD, t.PRICE_CHANGE_PCT_24H = s.PRICE_CHANGE_PCT_24H,
                    t.LOADED_AT = CURRENT_TIMESTAMP()
                WHEN NOT MATCHED THEN INSERT
                    (COIN_ID, COIN_NAME, SYMBOL, PRICE_DATE, PRICE_USD,
                     MARKET_CAP_USD, VOLUME_USD, PRICE_CHANGE_PCT_24H)
                VALUES (s.COIN_ID, s.COIN_NAME, s.SYMBOL, s.PRICE_DATE,
                        s.PRICE_USD, s.MARKET_CAP_USD, s.VOLUME_USD, s.PRICE_CHANGE_PCT_24H)
            """)
            print(f"Merged {len(price_rows)} price records")

        # ── Load news (MERGE to avoid duplicate story IDs) ──
        if news_rows:
            cur.execute("""
                CREATE OR REPLACE TEMPORARY TABLE CRYPTO_SENTIMENT_DB.ANALYTICS.NEWS_STAGING (
                    STORY_ID NUMBER, TITLE VARCHAR, URL VARCHAR, AUTHOR VARCHAR,
                    SCORE NUMBER, NUM_COMMENTS NUMBER, STORY_TEXT VARCHAR,
                    PUBLISHED_AT TIMESTAMP_NTZ, CATEGORY VARCHAR
                )
            """)
            cur.executemany(
                "INSERT INTO CRYPTO_SENTIMENT_DB.ANALYTICS.NEWS_STAGING VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                news_rows
            )
            cur.execute("""
                MERGE INTO CRYPTO_SENTIMENT_DB.ANALYTICS.TECH_NEWS t
                USING CRYPTO_SENTIMENT_DB.ANALYTICS.NEWS_STAGING s
                ON t.STORY_ID = s.STORY_ID
                WHEN NOT MATCHED THEN INSERT
                    (STORY_ID, TITLE, URL, AUTHOR, SCORE, NUM_COMMENTS,
                     STORY_TEXT, PUBLISHED_AT, CATEGORY)
                VALUES (s.STORY_ID, s.TITLE, s.URL, s.AUTHOR, s.SCORE,
                        s.NUM_COMMENTS, s.STORY_TEXT, s.PUBLISHED_AT, s.CATEGORY)
            """)
            print(f"Merged {len(news_rows)} news records")

        # ── Run sentiment on new articles ──
        cur.execute("""
            UPDATE CRYPTO_SENTIMENT_DB.ANALYTICS.TECH_NEWS
            SET SENTIMENT_SCORE = SNOWFLAKE.CORTEX.SENTIMENT(STORY_TEXT),
                SENTIMENT_LABEL = CASE
                    WHEN SNOWFLAKE.CORTEX.SENTIMENT(STORY_TEXT) >= 0.3 THEN 'Positive'
                    WHEN SNOWFLAKE.CORTEX.SENTIMENT(STORY_TEXT) <= -0.3 THEN 'Negative'
                    ELSE 'Neutral'
                END
            WHERE SENTIMENT_SCORE IS NULL AND STORY_TEXT IS NOT NULL
        """)
        print("Sentiment scoring complete")

    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    print("=== Crypto Sentiment Agent - Data Loader ===")
    print(f"Run time: {datetime.now(timezone.utc).isoformat()}")
    print()

    print("[1/3] Fetching crypto prices from CoinGecko...")
    prices = fetch_crypto_prices()

    print("[2/3] Fetching tech news from HackerNews...")
    news = fetch_hackernews_stories()

    print("[3/3] Loading into Snowflake...")
    load_to_snowflake(prices, news)

    print()
    print("Done! Your data is fresh.")
