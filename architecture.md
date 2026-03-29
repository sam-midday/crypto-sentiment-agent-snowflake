```
══════════════════════════════════════════════════════════════
                    DATA INGESTION
══════════════════════════════════════════════════════════════

  CoinGecko API          HackerNews API
  (crypto prices)        (tech articles)
       │                      │
       └──────┐    ┌──────────┘
              ▼    ▼
        Python Data Loader
        (MERGE / upsert)

══════════════════════════════════════════════════════════════
                      SNOWFLAKE
══════════════════════════════════════════════════════════════

  ┌─────────────────────┐    ┌─────────────────────────────┐
  │  CRYPTO_PRICES      │    │  TECH_NEWS                  │
  │  (structured data)  │    │  (unstructured + sentiment) │
  └────────┬────────────┘    └─────────────┬───────────────┘
           │                               │
           ▼                               ▼
  ┌─────────────────────┐    ┌─────────────────────────────┐
  │  Semantic View      │    │  Cortex Search Service      │
  │  (schema/metrics)   │    │  (full-text + vector search)│
  └────────┬────────────┘    └─────────────┬───────────────┘
           │                               │
           └──────────┐    ┌───────────────┘
                      ▼    ▼
              ┌──────────────────┐
              │  Cortex Agent    │
              │  (orchestrator)  │
              └────────┬─────────┘
                       │
                       ▼
              ┌──────────────────┐
              │  Streamlit App   │
              │  (chat UI)       │
              └──────────────────┘

══════════════════════════════════════════════════════════════
```

## Execution Order

| Step | File | What It Does |
|------|------|-------------|
| 1 | `snowflake/01_setup.sql` | Create warehouse, database, schema |
| 2 | `snowflake/02_tables.sql` | Create `CRYPTO_PRICES` and `TECH_NEWS` tables |
| 3 | `data_loader/load_crypto_data.py` | Fetch from APIs and MERGE into Snowflake |
| 4 | `snowflake/06_sentiment_scoring.sql` | Score sentiment on news articles via Cortex AI |
| 5 | `snowflake/03_semantic_view.sql` | Create Semantic View over price data |
| 6 | `snowflake/04_cortex_search.sql` | Create Cortex Search Service over news |
| 7 | `snowflake/05_cortex_agent.sql` | Wire up Cortex Agent with both tools |
| 8 | `streamlit/streamlit_app.py` | Deploy the chat UI |

Can also be run as per the sequence mentioned in README
