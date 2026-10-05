import os
from pathlib import Path

import pandas as pd
import streamlit as st

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# ============================================================
# STEP 10 - STREAMLIT DASHBOARD
# Chạy trực tiếp trên Windows host
# ============================================================

st.set_page_config(
    page_title="E-commerce Data Lakehouse Dashboard",
    page_icon="📊",
    layout="wide"
)


# ============================================================
# 10.1. LOAD .ENV
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parent.parent
ENV_PATH = PROJECT_ROOT / ".env"

load_dotenv(ENV_PATH)

MINIO_ACCESS_KEY = (
    os.environ.get("AWS_ACCESS_KEY_ID")
    or os.environ.get("MINIO_ROOT_USER")
)

MINIO_SECRET_KEY = (
    os.environ.get("AWS_SECRET_ACCESS_KEY")
    or os.environ.get("MINIO_ROOT_PASSWORD")
)

if not MINIO_ACCESS_KEY or not MINIO_SECRET_KEY:
    raise RuntimeError(
        f"Không tìm thấy credential MinIO trong biến môi trường hoặc file: {ENV_PATH}"
    )


# ============================================================
# 10.2. SPARK SESSION
# ============================================================

@st.cache_resource
def get_spark():
    ICEBERG_VERSION = "1.11.0"
    HADOOP_VERSION = "3.4.2"

    PACKAGES = (
        f"org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:{ICEBERG_VERSION},"
        f"org.apache.iceberg:iceberg-aws-bundle:{ICEBERG_VERSION},"
        f"org.apache.hadoop:hadoop-aws:{HADOOP_VERSION}"
    )

    spark = (
        SparkSession.builder
        .appName("EcommerceStreamlitDashboard")
        .master("local[*]")

        # Packages
        .config("spark.jars.packages", PACKAGES)

        # Iceberg extensions
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
        )

        # Iceberg REST Catalog
        .config(
            "spark.sql.catalog.lakehouse",
            "org.apache.iceberg.spark.SparkCatalog"
        )
        .config(
            "spark.sql.catalog.lakehouse.type",
            "rest"
        )

        # Vì dashboard chạy trên Windows host
        .config(
            "spark.sql.catalog.lakehouse.uri",
            "http://localhost:8181"
        )

        # Iceberg S3FileIO -> MinIO
        .config(
            "spark.sql.catalog.lakehouse.io-impl",
            "org.apache.iceberg.aws.s3.S3FileIO"
        )
        .config(
            "spark.sql.catalog.lakehouse.s3.endpoint",
            "http://localhost:9000"
        )
        .config(
            "spark.sql.catalog.lakehouse.s3.path-style-access",
            "true"
        )

        # S3A -> MinIO
        .config(
            "spark.hadoop.fs.s3a.endpoint",
            "http://localhost:9000"
        )
        .config(
            "spark.hadoop.fs.s3a.endpoint.region",
            "us-east-1"
        )
        .config(
            "spark.hadoop.fs.s3a.path.style.access",
            "true"
        )
        .config(
            "spark.hadoop.fs.s3a.connection.ssl.enabled",
            "false"
        )
        .config(
            "spark.hadoop.fs.s3a.access.key",
            MINIO_ACCESS_KEY
        )
        .config(
            "spark.hadoop.fs.s3a.secret.key",
            MINIO_SECRET_KEY
        )
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
        )
        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem"
        )

        .getOrCreate()
    )

    return spark


try:
    spark = get_spark()
except Exception as exc:
    st.error("Không thể khởi tạo SparkSession.")
    st.exception(exc)
    st.stop()


# ============================================================
# 10.3. DATA LOADING
# ============================================================

@st.cache_data(ttl=300)
def load_dashboard_kpi():
    return (
        spark.table("lakehouse.gold.dashboard_kpi")
        .limit(1)
        .toPandas()
    )


@st.cache_data(ttl=300)
def load_monthly_sales():
    return (
        spark.table("lakehouse.gold.monthly_sales")
        .orderBy("year_month")
        .toPandas()
    )


@st.cache_data(ttl=300)
def load_category_sales():
    return (
        spark.table("lakehouse.gold.category_sales")
        .filter(F.col("product_category_name").isNotNull())
        .orderBy(F.col("total_revenue").desc())
        .toPandas()
    )


@st.cache_data(ttl=300)
def load_product_performance():
    return (
        spark.table("lakehouse.gold.product_performance")
        .orderBy(F.col("product_revenue").desc())
        .limit(20)
        .toPandas()
    )


@st.cache_data(ttl=300)
def load_state_sales():
    return (
        spark.table("lakehouse.gold.state_sales")
        .filter(F.col("customer_state").isNotNull())
        .orderBy(F.col("total_revenue").desc())
        .toPandas()
    )


@st.cache_data(ttl=300)
def load_review_summary():
    return (
        spark.table("lakehouse.gold.review_summary")
        .orderBy("review_score")
        .toPandas()
    )


@st.cache_data(ttl=300)
def load_payment_type_analysis():
    return (
        spark.table("lakehouse.silver.payments")
        .groupBy("payment_type")
        .agg(
            F.count("*").alias("payment_records"),
            F.countDistinct("order_id").alias("total_orders"),
            F.round(
                F.sum("payment_value"),
                2
            ).alias("total_payment_value")
        )
        .orderBy(F.col("total_payment_value").desc())
        .toPandas()
    )


@st.cache_data(ttl=300)
def load_order_status():
    orders = spark.table("lakehouse.silver.orders")
    total_orders = orders.count()

    return (
        orders
        .groupBy("order_status")
        .agg(
            F.count("*").alias("total_orders")
        )
        .withColumn(
            "percentage",
            F.round(
                F.col("total_orders")
                / F.lit(total_orders)
                * 100,
                2
            )
        )
        .orderBy(F.col("total_orders").desc())
        .toPandas()
    )


# ============================================================
# 10.4. HEADER
# ============================================================

st.title("📊 E-commerce Data Lakehouse Dashboard")

st.caption(
    "Nguồn dữ liệu: Apache Iceberg trên MinIO, xử lý bằng Apache Spark."
)


# ============================================================
# 10.5. SIDEBAR
# ============================================================

st.sidebar.header("Bộ lọc")

top_n = st.sidebar.slider(
    "Số lượng Top Category / Product",
    min_value=5,
    max_value=20,
    value=10,
    step=1
)

show_tables = st.sidebar.checkbox(
    "Hiển thị bảng dữ liệu chi tiết",
    value=False
)

if st.sidebar.button("Làm mới dữ liệu"):
    st.cache_data.clear()
    st.rerun()


# ============================================================
# 10.6. KPI OVERVIEW
# ============================================================

try:
    kpi_df = load_dashboard_kpi()

    if kpi_df.empty:
        st.warning("Bảng gold.dashboard_kpi đang trống.")
    else:
        kpi = kpi_df.iloc[0]

        st.subheader("Tổng quan")

        col1, col2, col3 = st.columns(3)

        col1.metric(
            "Total Orders",
            f"{int(kpi['total_orders']):,}"
        )

        col2.metric(
            "Total Customers",
            f"{int(kpi['total_customers']):,}"
        )

        col3.metric(
            "Total Products",
            f"{int(kpi['total_products']):,}"
        )

        col4, col5, col6 = st.columns(3)

        col4.metric(
            "Total Revenue",
            f"${float(kpi['total_revenue']):,.2f}"
        )

        col5.metric(
            "Average Order Value",
            f"${float(kpi['avg_order_value']):,.2f}"
        )

        col6.metric(
            "Average Review Score",
            f"{float(kpi['avg_review_score']):.2f} / 5"
        )

except Exception as exc:
    st.error(
        "Không thể đọc gold.dashboard_kpi. "
        "Hãy kiểm tra Step 09 đã chạy xong chưa."
    )
    st.exception(exc)
    st.stop()


st.divider()


# ============================================================
# 10.7. MONTHLY REVENUE
# ============================================================

st.subheader("📈 Doanh thu theo tháng")

monthly_df = load_monthly_sales()

if not monthly_df.empty:
    monthly_chart = monthly_df[
        ["year_month", "total_revenue"]
    ].copy()

    monthly_chart["year_month"] = monthly_chart[
        "year_month"
    ].astype(str)

    st.line_chart(
        monthly_chart,
        x="year_month",
        y="total_revenue"
    )

    if show_tables:
        st.dataframe(
            monthly_df,
            use_container_width=True,
            hide_index=True
        )
else:
    st.info("Chưa có dữ liệu monthly_sales.")


st.divider()


# ============================================================
# 10.8. CATEGORY + PRODUCT
# ============================================================

left, right = st.columns(2)

with left:
    st.subheader("🏷️ Top Categories")

    category_df = load_category_sales().head(top_n)

    if not category_df.empty:
        st.bar_chart(
            category_df,
            x="product_category_name",
            y="total_revenue"
        )

        if show_tables:
            st.dataframe(
                category_df[
                    [
                        "product_category_name",
                        "total_orders",
                        "items_sold",
                        "total_revenue"
                    ]
                ],
                use_container_width=True,
                hide_index=True
            )

with right:
    st.subheader("📦 Top Products")

    product_df = load_product_performance().head(top_n)

    if not product_df.empty:
        st.bar_chart(
            product_df,
            x="product_id",
            y="product_revenue"
        )

        if show_tables:
            st.dataframe(
                product_df[
                    [
                        "product_id",
                        "product_category_name",
                        "items_sold",
                        "product_revenue"
                    ]
                ],
                use_container_width=True,
                hide_index=True
            )


st.divider()


# ============================================================
# 10.9. STATE SALES
# ============================================================

st.subheader("🗺️ Doanh thu theo bang")

state_df = load_state_sales()

if not state_df.empty:
    state_options = ["All"] + state_df[
        "customer_state"
    ].astype(str).tolist()

    selected_state = st.selectbox(
        "Chọn bang",
        state_options
    )

    if selected_state == "All":
        state_display = state_df
    else:
        state_display = state_df[
            state_df["customer_state"] == selected_state
        ]

    st.bar_chart(
        state_display,
        x="customer_state",
        y="total_revenue"
    )

    if selected_state != "All":
        row = state_display.iloc[0]

        a, b, c = st.columns(3)

        a.metric(
            "Orders",
            f"{int(row['total_orders']):,}"
        )

        b.metric(
            "Items",
            f"{int(row['total_items']):,}"
        )

        c.metric(
            "Revenue",
            f"${float(row['total_revenue']):,.2f}"
        )

    if show_tables:
        st.dataframe(
            state_display,
            use_container_width=True,
            hide_index=True
        )


st.divider()


# ============================================================
# 10.10. REVIEW + PAYMENT
# ============================================================

left, right = st.columns(2)

with left:
    st.subheader("⭐ Review Distribution")

    review_df = load_review_summary()

    if not review_df.empty:
        review_chart = review_df.copy()

        review_chart["review_score"] = (
            review_chart["review_score"]
            .astype(str)
        )

        st.bar_chart(
            review_chart,
            x="review_score",
            y="total_reviews"
        )

        if show_tables:
            st.dataframe(
                review_df,
                use_container_width=True,
                hide_index=True
            )

with right:
    st.subheader("💳 Payment Methods")

    payment_df = load_payment_type_analysis()

    if not payment_df.empty:
        st.bar_chart(
            payment_df,
            x="payment_type",
            y="total_payment_value"
        )

        if show_tables:
            st.dataframe(
                payment_df,
                use_container_width=True,
                hide_index=True
            )


st.divider()


# ============================================================
# 10.11. ORDER STATUS
# ============================================================

st.subheader("🚚 Order Status")

status_df = load_order_status()

if not status_df.empty:
    st.bar_chart(
        status_df,
        x="order_status",
        y="total_orders"
    )

    if show_tables:
        st.dataframe(
            status_df,
            use_container_width=True,
            hide_index=True
        )


st.divider()


# ============================================================
# 10.12. ARCHITECTURE
# ============================================================

with st.expander("Kiến trúc Data Lakehouse"):
    st.code(
        """
Raw CSV
   ↓
MinIO Landing
   ↓
Bronze - Iceberg
   ↓
Silver - Cleaned Iceberg Tables
   ↓
Gold - Analytics Tables
   ↓
Spark Analytics
   ↓
Streamlit Dashboard
        """,
        language="text"
    )


# ============================================================
# 10.13. FOOTER
# ============================================================

st.caption(
    "E-commerce Data Lakehouse | "
    "MinIO + Apache Spark + Apache Iceberg + Streamlit"
)
