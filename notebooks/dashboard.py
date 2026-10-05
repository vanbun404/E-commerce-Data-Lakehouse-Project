import os

import pandas as pd
import streamlit as st

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# ============================================================
# BƯỚC 10 - BẢNG ĐIỀU KHIỂN STREAMLIT
# ============================================================

st.set_page_config(
    page_title="Bảng điều khiển Data Lakehouse E-commerce",
    page_icon="📊",
    layout="wide"
)


# ============================================================
# 10.1. PHIÊN SPARK
# ============================================================

@st.cache_resource
def get_spark():
    ICEBERG_VERSION = "1.11.0"
    HADOOP_VERSION = "3.4.2"

    packages = (
        f"org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:{ICEBERG_VERSION},"
        f"org.apache.iceberg:iceberg-aws-bundle:{ICEBERG_VERSION},"
        f"org.apache.hadoop:hadoop-aws:{HADOOP_VERSION}"
    )

    access_key = (
        os.environ.get("AWS_ACCESS_KEY_ID")
        or os.environ.get("MINIO_ROOT_USER")
    )

    secret_key = (
        os.environ.get("AWS_SECRET_ACCESS_KEY")
        or os.environ.get("MINIO_ROOT_PASSWORD")
    )

    if not access_key or not secret_key:
        raise RuntimeError(
            "Không tìm thấy credential MinIO trong biến môi trường."
        )

    spark = (
        SparkSession.builder
        .appName("EcommerceStreamlitDashboard")
        .master("local[*]")

        .config("spark.jars.packages", packages)

        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
        )

        .config(
            "spark.sql.catalog.lakehouse",
            "org.apache.iceberg.spark.SparkCatalog"
        )
        .config(
            "spark.sql.catalog.lakehouse.type",
            "rest"
        )
        .config(
            "spark.sql.catalog.lakehouse.uri",
            "http://iceberg-rest:8181"
        )

        .config(
            "spark.sql.catalog.lakehouse.io-impl",
            "org.apache.iceberg.aws.s3.S3FileIO"
        )
        .config(
            "spark.sql.catalog.lakehouse.s3.endpoint",
            "http://minio:9000"
        )
        .config(
            "spark.sql.catalog.lakehouse.s3.path-style-access",
            "true"
        )

        .config(
            "spark.hadoop.fs.s3a.endpoint",
            "http://minio:9000"
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
            access_key
        )
        .config(
            "spark.hadoop.fs.s3a.secret.key",
            secret_key
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


spark = get_spark()


# ============================================================
# 10.2. TẢI DỮ LIỆU
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
# 10.3. TIÊU ĐỀ
# ============================================================

st.title("📊 Bảng điều khiển Data Lakehouse E-commerce")

st.caption(
    "Nguồn dữ liệu: Lớp Gold của Apache Iceberg trên MinIO, "
    "xử lý bằng Apache Spark."
)


# ============================================================
# 10.4. THANH BÊN
# ============================================================

st.sidebar.header("Bộ lọc dữ liệu")

top_n = st.sidebar.slider(
    "Số lượng danh mục / sản phẩm hàng đầu",
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
# 10.5. TỔNG QUAN KPI
# ============================================================

try:
    kpi_df = load_dashboard_kpi()

    if kpi_df.empty:
        st.warning("Bảng gold.dashboard_kpi hiện đang trống.")
    else:
        kpi = kpi_df.iloc[0]

        st.subheader("Tổng quan hệ thống")

        col1, col2, col3 = st.columns(3)

        col1.metric(
            "Tổng số đơn hàng",
            f"{int(kpi['total_orders']):,}"
        )

        col2.metric(
            "Tổng số khách hàng",
            f"{int(kpi['total_customers']):,}"
        )

        col3.metric(
            "Tổng số sản phẩm",
            f"{int(kpi['total_products']):,}"
        )

        col4, col5, col6 = st.columns(3)

        col4.metric(
            "Tổng doanh thu",
            f"${float(kpi['total_revenue']):,.2f}"
        )

        col5.metric(
            "Giá trị đơn hàng trung bình",
            f"${float(kpi['avg_order_value']):,.2f}"
        )

        col6.metric(
            "Điểm đánh giá trung bình",
            f"{float(kpi['avg_review_score']):.2f} / 5"
        )

except Exception as exc:
    st.error(
        "Không thể đọc gold.dashboard_kpi. "
        "Hãy chạy hoàn tất Step 09 trước."
    )
    st.exception(exc)
    st.stop()


st.divider()


# ============================================================
# 10.6. DOANH THU THEO THÁNG
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
    st.info("Chưa có dữ liệu doanh thu theo tháng.")


st.divider()


# ============================================================
# 10.7. DANH MỤC + SẢN PHẨM
# ============================================================

left, right = st.columns(2)

with left:
    st.subheader("🏷️ Danh mục có doanh thu cao nhất")

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
    st.subheader("📦 Sản phẩm có doanh thu cao nhất")

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
# 10.8. DOANH THU THEO BANG
# ============================================================

st.subheader("🗺️ Doanh thu theo khu vực / bang")

state_df = load_state_sales()

if not state_df.empty:
    state_options = ["Tất cả"] + state_df[
        "customer_state"
    ].astype(str).tolist()

    selected_state = st.selectbox(
        "Chọn bang / khu vực",
        state_options
    )

    if selected_state == "Tất cả":
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

    if selected_state != "Tất cả":
        row = state_display.iloc[0]

        a, b, c = st.columns(3)

        a.metric(
            "Số đơn hàng",
            f"{int(row['total_orders']):,}"
        )

        b.metric(
            "Số sản phẩm",
            f"{int(row['total_items']):,}"
        )

        c.metric(
            "Doanh thu",
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
# 10.9. PHÂN BỐ ĐÁNH GIÁ
# ============================================================

left, right = st.columns(2)

with left:
    st.subheader("⭐ Phân bố điểm đánh giá")

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


# ============================================================
# 10.10. PHƯƠNG THỨC THANH TOÁN
# ============================================================

with right:
    st.subheader("💳 Phương thức thanh toán")

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
# 10.11. TRẠNG THÁI ĐƠN HÀNG
# ============================================================

st.subheader("🚚 Trạng thái đơn hàng")

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
# 10.12. KIẾN TRÚC HỆ THỐNG
# ============================================================

with st.expander("Kiến trúc hệ thống Data Lakehouse"):
    st.code(
        """
Dữ liệu CSV thô
   ↓
MinIO Landing
   ↓
Bronze - Bảng Iceberg thô
   ↓
Silver - Bảng Iceberg đã làm sạch
   ↓
Gold - Bảng phục vụ phân tích
   ↓
Phân tích bằng Spark
   ↓
Bảng điều khiển Streamlit
        """,
        language="text"
    )


# ============================================================
# 10.13. CHÂN TRANG
# ============================================================

st.caption(
    "Data Lakehouse E-commerce | "
    "MinIO + Apache Spark + Apache Iceberg + Streamlit"
)
