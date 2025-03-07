import streamlit as st 

conn = st.connection('snowflake')

st.title("Realtime Orders Streaming")

st.header("Orders Metrics",divider="blue")

df = conn.query("SELECT * FROM KAFKA_STREAM.PUBLIC.VW_ORDER_STATUS;", ttl="10m")

st.table(df)
