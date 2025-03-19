import streamlit as st 

conn = st.connection('snowflake')

st.set_page_config(
    page_title='Realtime Orders Streaming',
    layout="wide",
    initial_sidebar_state="expanded"
    )

st.logo(image="images/streamlit-logo-primary-colormark-lighttext.png",icon_image="images/streamlit-mark-color.png")

with st.sidebar: 
    st.title('Realtime Order Analytics 🛒')
    st.header(' Param Setting ⚙️')

df = conn.query("SELECT * FROM KAFKA_STREAM.PUBLIC.VW_ORDER_STATUS;", ttl="10m")

# st.table(df)

# Title
st.title("📦 Order Status Dashboard")

# Display metrics in columns
col1, col2, col3 = st.columns(3)
col1.metric("✅ Orders Delivered", df["ORDERS_DELIVERED"])
col2.metric("🚚 Orders Shipped",  df["ORDERS_SHIPPED"])
col3.metric("⏳ Orders Pending",  df["ORDERS_PENDING"])

col4, col5, col6 = st.columns(3)
col4.metric("🔄 Orders In Progress", df["ORDERS_INPROGRESS"])
col5.metric("❌ Orders Cancelled", df["ORDERS_CANCELLED"])
col6.metric("⚠️ Failed Transactions", df["ORDERS_WITH_TRANSACTION_FAILED"])

# Display progress bars for percentage values
st.subheader("📊 Order Completion Statistics")
st.write(f"✅ **Delivered Orders: {df.at[0,'PERCAENTAGE_ORDERS_DELIVERED']}**")
st.write(f"❌ **Cancelled Orders: {df.at[0,'PERCAENTAGE_ORDERS_CANCELLED']}**")

import streamlit as st
import pandas as pd
import plotly.express as px

# Fetch data dynamically from Snowflake (or any database)
# conn = st.connection("snowflake")  # Ensure you have a valid connection in Streamlit secrets
df = conn.query("SELECT * FROM KAFKA_STREAM.PUBLIC.VW_REVENUE_BY_POS;", ttl="10m")

# Rename columns for better readability
df.columns = ["POS", "REVENUE"]

# Display key revenue metrics
st.subheader("💰 Total Revenue Breakdown by POS")
top_pos = df.iloc[0]  # Get the top-performing POS
st.metric(f"🏆 Top POS: {top_pos['POS']}", f"₹{top_pos['REVENUE']:,.2f}")

# Display POS revenue as a bar chart
fig = px.bar(df, x="POS", y="REVENUE", text="REVENUE", title="POS-wise Revenue",
             labels={"POS": "Point of Sale", "REVENUE": "Revenue (₹)"},
             color="REVENUE", color_continuous_scale="blues")
fig.update_traces(texttemplate="₹%{text:,.2f}", textposition="outside")
fig.update_layout(xaxis_title="POS", yaxis_title="Revenue (₹)", yaxis_tickformat=",.2f")

st.plotly_chart(fig, use_container_width=True)

df = conn.query("SELECT * FROM KAFKA_STREAM.PUBLIC.VW_ORDERS_PER_REGION;", ttl="10m")

# Rename columns for better readability
df.columns = ["REGION", "COUNT"]


# Pie Chart using Plotly
fig = px.pie(df, names="REGION", values="COUNT", title="Orders by Region",
             color_discrete_sequence=px.colors.qualitative.Set2, hole=0.3)

st.plotly_chart(fig, use_container_width=True)


df = conn.query("SELECT DISTINCT CUSTOMER_ID,AVERAGE_ORDER_VALUE FROM KAFKA_STREAM.PUBLIC.VW_CUSTOMERS_METRICS;;", ttl="10m")


# Rename columns for better readability
df.columns = ["MOST_REPEATING_CUSTOMER", "AVERAGE_ORDER_VALUE"]


# Highlight the top customer
top_customer = df.iloc[0]
st.metric(f"🏆 Top Customer: {top_customer['MOST_REPEATING_CUSTOMER']}", f"₹{top_customer['AVERAGE_ORDER_VALUE']:,.2f}")

# Bar Chart using Plotly
fig = px.bar(df, x="MOST_REPEATING_CUSTOMER", y="AVERAGE_ORDER_VALUE",
             text="AVERAGE_ORDER_VALUE", title="Average Order Value by Customer",
             labels={"MOST_REPEATING_CUSTOMER": "Customer", "AVERAGE_ORDER_VALUE": "Avg Order Value (₹)"},
             color="AVERAGE_ORDER_VALUE", color_continuous_scale="blues")
fig.update_traces(texttemplate="₹%{text:,.2f}", textposition="outside")
fig.update_layout(xaxis_title="Customer", yaxis_title="Average Order Value (₹)", yaxis_tickformat=",.2f")

st.plotly_chart(fig, use_container_width=True)

df = conn.query("SELECT * FROM KAFKA_STREAM.PUBLIC.VW_MOSTSOLD_PRODUCT;", ttl="10m")

# Check if data is retrieved successfully
if df.empty:
    st.error("No data found. Please check the database connection or table.")
    st.stop()

# Rename columns for better readability
df.columns = ["PRODUCT_NAME", "BRAND", "TOTAL_SALES"]

# Sort by TOTAL_SALES in descending order
df = df.sort_values(by="TOTAL_SALES", ascending=False)

# Streamlit UI
st.title("🛍️ Product Sales Dashboard")

# Highlight the Top-Selling Product
top_product = df.iloc[0]
st.metric(f"🏆 Top-Selling Product: {top_product['PRODUCT_NAME']}", f"{top_product['TOTAL_SALES']} Units", f"Brand: {top_product['BRAND']}")

# Bar Chart using Plotly
fig = px.bar(df, x="PRODUCT_NAME", y="TOTAL_SALES", text="TOTAL_SALES", 
             color="BRAND", title="Total Sales by Product", labels={"PRODUCT_NAME": "Product", "TOTAL_SALES": "Total Sales"},
             color_discrete_sequence=px.colors.qualitative.Vivid)

fig.update_traces(texttemplate="%{text}", textposition="outside")
fig.update_layout(xaxis_title="Product", yaxis_title="Total Sales", yaxis_tickformat=",d")

st.plotly_chart(fig, use_container_width=True)

# Custom Table with Conditional Formatting
st.subheader("📋 Product Sales Data")

def highlight_max(s):
    """Highlights the max value in TOTAL_SALES."""
    is_max = s == s.max()
    return ["background-color: lightgreen; font-weight: bold" if v else "" for v in is_max]

styled_df = df.style.apply(highlight_max, subset=["TOTAL_SALES"]).set_properties(**{
    "background-color": "#f9f9f9",
    "border": "1px solid #ddd",
    "text-align": "center",
})

st.dataframe(styled_df, width=700)

