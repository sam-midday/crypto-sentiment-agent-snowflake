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

SNOWFLAKE_CONFIG = {
    "account":   "YOUR_ACCOUNT",
    "user":      "YOUR_USERNAME",
    "password":  "YOUR_PASSWORD",
    "warehouse": "CRYPTO_WH",
    "database":  "CRYPTO_SENTIMENT_DB",
    "schema":    "ANALYTICS",
}

COINS = ["bitcoin", "ethereum", "solana", "cardano", "dogecoin"]
HN_SEARCH_TERMS = ["crypto", "bitcoin", "ethereum", "blockchain", "defi", "AI"]


def fetch_crypto_prices():
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
            coin.get("high_24h"),
            coin.get("low_24h"),
        ))
    return rows


def fetch_hackernews_articles():
    rows = []
    seen = set()
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    for term in HN_SEARCH_TERMS:
        url = "https://hn.algolia.com/api/v1/search_by_date"
        params = {"query": term, "tags": "story", "hitsPerPage": 10}
        resp = requests.get(url, params=params, timeout=30)
        resp.raise_for_status()
        hits = resp.json().get("hits", [])

        for h in hits:
            story_id = str(h.get("objectID", ""))
            if story_id in seen:
                continue
            seen.add(story_id)

            title = h.get("title") or ""
            article_url = h.get("url") or ""
            author = h.get("author") or ""
            points = h.get("points") or 0
            num_comments = h.get("num_comments") or 0
            created_at = h.get("created_at") or today

            content = f"{title}. Source: HackerNews. Author: {author}. Points: {points}."

            rows.append((
                story_id,
                title,
                article_url,
                author,
                points,
                num_comments,
                created_at,
                today,
                content,
                term,
            ))
    return rows


def upsert_crypto_prices(conn, rows):
    cur = conn.cursor()
    cur.execute("""
        CREATE TEMPORARY TABLE CRYPTO_PRICES_STAGING LIKE CRYPTO_SENTIMENT_DB.ANALYTICS.CRYPTO_PRICES
    """)
    cur.executemany("""
        INSERT INTO CRYPTO_PRICES_STAGING
            (COIN_ID, COIN_NAME, SYMBOL, PRICE_DATE, CURRENT_PRICE,
             MARKET_CAP, TOTAL_VOLUME, PRICE_CHANGE_PCT_24H, HIGH_24H, LOW_24H)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, rows)
    cur.execute("""
        MERGE INTO CRYPTO_SENTIMENT_DB.ANALYTICS.CRYPTO_PRICES AS tgt
        USING CRYPTO_PRICES_STAGING AS src
          ON tgt.COIN_ID = src.COIN_ID AND tgt.PRICE_DATE = src.PRICE_DATE
        WHEN MATCHED THEN UPDATE SET
          tgt.CURRENT_PRICE         = src.CURRENT_PRICE,
          tgt.MARKET_CAP            = src.MARKET_CAP,
          tgt.TOTAL_VOLUME          = src.TOTAL_VOLUME,
          tgt.PRICE_CHANGE_PCT_24H  = src.PRICE_CHANGE_PCT_24H,
          tgt.HIGH_24H              = src.HIGH_24H,
          tgt.LOW_24H               = src.LOW_24H
        WHEN NOT MATCHED THEN INSERT
          (COIN_ID, COIN_NAME, SYMBOL, PRICE_DATE, CURRENT_PRICE,
           MARKET_CAP, TOTAL_VOLUME, PRICE_CHANGE_PCT_24H, HIGH_24H, LOW_24H)
        VALUES
          (src.COIN_ID, src.COIN_NAME, src.SYMBOL, src.PRICE_DATE, src.CURRENT_PRICE,
           src.MARKET_CAP, src.TOTAL_VOLUME, src.PRICE_CHANGE_PCT_24H, src.HIGH_24H, src.LOW_24H)
    """)
    print(f"  Upserted {len(rows)} crypto price rows.")
    cur.close()


def upsert_tech_news(conn, rows):
    cur = conn.cursor()
    cur.execute("""
        CREATE TEMPORARY TABLE TECH_NEWS_STAGING LIKE CRYPTO_SENTIMENT_DB.ANALYTICS.TECH_NEWS
    """)
    cur.executemany("""
        INSERT INTO TECH_NEWS_STAGING
            (STORY_ID, TITLE, URL, AUTHOR, POINTS, NUM_COMMENTS,
             CREATED_AT, LOAD_DATE, CONTENT, SEARCH_TERM)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, rows)
    cur.execute("""
        MERGE INTO CRYPTO_SENTIMENT_DB.ANALYTICS.TECH_NEWS AS tgt
        USING TECH_NEWS_STAGING AS src
          ON tgt.STORY_ID = src.STORY_ID
        WHEN MATCHED THEN UPDATE SET
          tgt.TITLE        = src.TITLE,
          tgt.URL          = src.URL,
          tgt.AUTHOR       = src.AUTHOR,
          tgt.POINTS       = src.POINTS,
          tgt.NUM_COMMENTS = src.NUM_COMMENTS,
          tgt.CONTENT      = src.CONTENT,
          tgt.SEARCH_TERM  = src.SEARCH_TERM
        WHEN NOT MATCHED THEN INSERT
          (STORY_ID, TITLE, URL, AUTHOR, POINTS, NUM_COMMENTS,
           CREATED_AT, LOAD_DATE, CONTENT, SEARCH_TERM)
        VALUES
          (src.STORY_ID, src.TITLE, src.URL, src.AUTHOR, src.POINTS, src.NUM_COMMENTS,
           src.CREATED_AT, src.LOAD_DATE, src.CONTENT, src.SEARCH_TERM)
    """)
    print(f"  Upserted {len(rows)} tech news rows.")
    cur.close()


def main():
    print("=" * 60)
    print("Crypto Sentiment Agent - Data Loader")
    print("=" * 60)

    print("\n[1/4] Fetching crypto prices from CoinGecko...")
    crypto_rows = fetch_crypto_prices()
    print(f"  Fetched {len(crypto_rows)} coins.")

    print("\n[2/4] Fetching tech news from HackerNews...")
    news_rows = fetch_hackernews_articles()
    print(f"  Fetched {len(news_rows)} articles.")

    print("\n[3/4] Connecting to Snowflake...")
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    print("  Connected.")

    print("\n[4/4] Loading data into Snowflake...")
    upsert_crypto_prices(conn, crypto_rows)
    upsert_tech_news(conn, news_rows)

    conn.close()
    print("\nDone!")


if __name__ == "__main__":
    main()

