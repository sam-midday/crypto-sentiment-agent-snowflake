import streamlit as st
import json
import re
from snowflake.snowpark.context import get_active_session

st.set_page_config(page_title="Crypto Sentiment Agent", page_icon="📊", layout="wide")

session = get_active_session()

st.title("📊 Crypto Market Sentiment Agent")
st.caption("Ask me about crypto prices, market trends, or news sentiment!")

AGENT_NAME = "CRYPTO_SENTIMENT_DB.ANALYTICS.CRYPTO_AGENT"

SAMPLE_QUESTIONS = [
    "What is the current price of Bitcoin?",
    "Which crypto had the biggest gain recently?",
    "Any news about DeFi security incidents?",
    "What is the overall market sentiment?",
    "Compare Ethereum and Solana prices",
    "Show me the market cap of all coins",
]

with st.sidebar:
    st.header("Sample Questions")
    selected_sample = st.selectbox("Try a sample question:", [""] + SAMPLE_QUESTIONS)

    st.divider()
    st.header("About")
    st.markdown("""
    This agent combines:
    - **Cortex Analyst** for price/volume data
    - **Cortex Search** for news & sentiment
    - **AI Sentiment Scoring** on articles

    Built with Snowflake Cortex Agent.
    """)

question = st.text_input(
    "Ask about crypto prices, news, or sentiment...",
    value=selected_sample if selected_sample else "",
)


def clean_text(text):
    text = text.replace("$", "\\$")
    text = re.sub(r"\s*[′']\s*", "'", text)
    text = re.sub(r"(\w),(\w)", r"\1, \2", text)
    text = text.replace("\\n", "\n")
    return text


if st.button("Ask", use_container_width=True) and question:
    with st.spinner("Thinking..."):
        request_body = json.dumps({
            "messages": [
                {
                    "role": "user",
                    "content": [{"type": "text", "text": question}]
                }
            ]
        })

        result = session.sql(f"""
            SELECT SNOWFLAKE.CORTEX.DATA_AGENT_RUN(
                '{AGENT_NAME}',
                $${request_body}$$
            ) AS response
        """).collect(statement_params={"STATEMENT_TIMEOUT_IN_SECONDS": "120"})

        raw_response = result[0]["RESPONSE"]

        try:
            response_json = json.loads(raw_response)

            if "message" in response_json and "code" in response_json:
                answer = f"**Error:** {response_json['message']}"
            else:
                content_items = response_json.get("content", [])
                text_parts = []
                for item in content_items:
                    if item.get("type") == "text":
                        text_parts.append(item.get("text", ""))
                    elif item.get("type") == "tool_result":
                        tr = item.get("tool_result", {})
                        for c in tr.get("content", []):
                            if c.get("type") == "json":
                                j = c.get("json", {})
                                if "result_set" in j:
                                    rs = j["result_set"]
                                    meta = rs.get("resultSetMetaData", {})
                                    cols = [r["name"] for r in meta.get("rowType", [])]
                                    data = rs.get("data", [])
                                    if cols and data:
                                        import pandas as pd
                                        df = pd.DataFrame(data, columns=cols)
                                        st.dataframe(df, use_container_width=True)

                answer = "\n\n".join(text_parts) if text_parts else "I processed your request but didn't generate a text response."

        except (json.JSONDecodeError, KeyError):
            answer = raw_response

    st.divider()
    st.markdown("**Agent:**")
    st.markdown(clean_text(answer))
