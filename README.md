# 📊 Crypto Market Sentiment Agent

An end-to-end Agentic AI application built on **Snowflake Cortex** that combines real-time cryptocurrency market data with AI-powered news sentiment analysis. Ask questions in plain English and get intelligent, data-driven answers.

![App Screenshot](screenshots/app_demo.png)


## ✨ Features

- **Natural Language Queries** — Ask "What's Bitcoin's price?" and get SQL-generated answers via Cortex Analyst
- **Semantic News Search** — Ask "Any DeFi security news?" and get relevant articles via Cortex Search
- **AI Sentiment Scoring** — Every news article is auto-scored using `SNOWFLAKE.CORTEX.SENTIMENT()`
- **Agentic Orchestration** — Cortex Agent automatically picks the right tool (Analyst vs Search) per question
- **Live Data Pipeline** — Python script fetches from free APIs; runs daily via cron
- **Interactive UI** — Streamlit in Snowflake chat interface with sample questions

## 🛠️ Tech Stack

| Component | Technology |
|---|---|
| Cloud Platform | Snowflake |
| AI/ML | Snowflake Cortex (Analyst, Search, Agent, Sentiment) |
| Data Sources | CoinGecko API (free), HackerNews API (free) |
| Data Loading | Python + snowflake-connector-python |
| Frontend | Streamlit in Snowflake |
| Data Modeling | Snowflake Semantic Views |

## 🚀 Setup Instructions

### Prerequisites
- Snowflake account (trial works for most features)
- Python 3.8+
- `pip install snowflake-connector-python requests`
