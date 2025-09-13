import pandas as pd
import streamlit as st
import plotly.express as px

DATA_PATH = "data/jobs_curated_clean.csv"

@st.cache_data
def load_data():
    return pd.read_csv(DATA_PATH, parse_dates=["created_date"])

def main():
    st.set_page_config(page_title="Job Analytics Dashboard", layout="wide")
    st.title("📊 Job Analytics Dashboard")

    df = load_data()

    # Sidebar filters
    st.sidebar.header("Filters")
    companies = st.sidebar.multiselect("Select companies", df["company"].unique())
    categories = st.sidebar.multiselect("Select categories", df["category"].unique())

    filtered_df = df.copy()
    if companies:
        filtered_df = filtered_df[filtered_df["company"].isin(companies)]
    if categories:
        filtered_df = filtered_df[filtered_df["category"].isin(categories)]

    st.subheader("Dataset Preview")
    st.dataframe(filtered_df.head(10))

    # Salary distribution
    st.subheader("💰 Salary Distribution")
    fig = px.histogram(filtered_df, x="salary_min", nbins=10, title="Salary Min Distribution")
    st.plotly_chart(fig, use_container_width=True)

    # Jobs by Category
    st.subheader("📚 Jobs by Category")
    cat_counts = filtered_df["category"].value_counts().reset_index()
    cat_counts.columns = ["category", "count"]
    fig = px.bar(cat_counts, x="category", y="count", title="Jobs per Category")
    st.plotly_chart(fig, use_container_width=True)

    # Jobs by Company
    st.subheader("🏢 Jobs by Company")
    comp_counts = filtered_df["company"].value_counts().reset_index()
    comp_counts.columns = ["company", "count"]
    fig = px.bar(comp_counts, x="company", y="count", title="Jobs per Company")
    st.plotly_chart(fig, use_container_width=True)

    # Jobs over time
    st.subheader("📅 Jobs Over Time")
    if "created_date" in filtered_df.columns:
        time_counts = filtered_df.groupby(filtered_df["created_date"].dt.date).size().reset_index(name="count")
        fig = px.line(time_counts, x="created_date", y="count", title="Jobs Posted Over Time")
        st.plotly_chart(fig, use_container_width=True)

if __name__ == "__main__":
    main()
