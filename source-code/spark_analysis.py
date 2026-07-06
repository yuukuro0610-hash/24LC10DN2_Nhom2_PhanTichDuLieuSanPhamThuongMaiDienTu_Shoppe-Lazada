"""
PySpark Analysis — Phân tích dữ liệu Shopee/Lazada
Bao gồm 5 phân tích tương đương 5 chương trình MapReduce:
  1. Đếm sản phẩm theo ngành hàng
  2. Giá trung bình theo ngành hàng
  3. Top sản phẩm bán chạy
  4. Top thương hiệu bán nhiều nhất
  5. Phân bố đánh giá sao

Yêu cầu: pip install pyspark
Chạy   : spark-submit spark_analysis.py
         hoặc python spark_analysis.py (local mode)
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ──────────────────────────────────────────────────────────────────────────────
# Khởi tạo SparkSession
# ──────────────────────────────────────────────────────────────────────────────
spark = (
    SparkSession.builder
    .appName("ECommerce_Analysis")
    .master("local[*]")
    .config("spark.driver.memory", "2g")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

ITEMS_PATH   = "cleaned_lazada_items.csv"
REVIEWS_PATH = "cleaned_lazada_reviews.csv"
SHOPEE_PATH  = "cleaned_shopee.csv"

CATEGORY_MAP = {
    "beli-harddisk-eksternal": "External HDD",
    "jual-flash-drives":       "Flash Drives",
    "beli-smart-tv":           "Smart TV",
    "shop-televisi-digital":   "Digital TV",
    "beli-laptop":             "Laptop",
}

# ──────────────────────────────────────────────────────────────────────────────
# Đọc dữ liệu
# ──────────────────────────────────────────────────────────────────────────────
print("\n" + "=" * 65)
print("  ĐỌC DỮ LIỆU")
print("=" * 65)

df_items = (
    spark.read.csv(ITEMS_PATH, header=True, inferSchema=True)
    .withColumn(
        "category_name",
        F.create_map(*[x for pair in CATEGORY_MAP.items() for x in pair])[F.col("category")]
    )
)

df_reviews = spark.read.csv(REVIEWS_PATH, header=True, inferSchema=True)
df_shopee  = spark.read.csv(SHOPEE_PATH,  header=True, inferSchema=True)

print(f"  Lazada items   : {df_items.count():>8,} dòng")
print(f"  Lazada reviews : {df_reviews.count():>8,} dòng")
print(f"  Shopee reviews : {df_shopee.count():>8,} dòng")

# ──────────────────────────────────────────────────────────────────────────────
# Phân tích 1 — Đếm sản phẩm theo ngành hàng
# ──────────────────────────────────────────────────────────────────────────────
print("\n" + "=" * 65)
print("  PHÂN TÍCH 1 — Đếm sản phẩm theo ngành hàng")
print("=" * 65)

cat_count = (
    df_items
    .groupBy("category_name")
    .agg(F.count("*").alias("so_san_pham"))
    .orderBy(F.desc("so_san_pham"))
)
cat_count.show(truncate=False)

# ──────────────────────────────────────────────────────────────────────────────
# Phân tích 2 — Giá trung bình theo ngành hàng
# ──────────────────────────────────────────────────────────────────────────────
print("=" * 65)
print("  PHÂN TÍCH 2 — Giá trung bình theo ngành hàng")
print("=" * 65)

avg_price = (
    df_items
    .filter(F.col("price") > 0)
    .groupBy("category_name")
    .agg(
        F.round(F.avg("price"), 0).alias("gia_trung_binh_Rp"),
        F.min("price").alias("gia_thap_nhat_Rp"),
        F.max("price").alias("gia_cao_nhat_Rp"),
        F.count("*").alias("so_san_pham"),
    )
    .orderBy(F.desc("gia_trung_binh_Rp"))
)
avg_price.show(truncate=False)

# ──────────────────────────────────────────────────────────────────────────────
# Phân tích 3 — Top sản phẩm bán chạy
# ──────────────────────────────────────────────────────────────────────────────
print("=" * 65)
print("  PHÂN TÍCH 3 — Top 10 sản phẩm bán chạy nhất")
print("=" * 65)

top_products = (
    df_items
    .groupBy("name", "category_name", "price")
    .agg(F.max("totalReviews").alias("tong_luot_review"))
    .orderBy(F.desc("tong_luot_review"))
    .limit(10)
    .select(
        F.col("name"),
        F.col("category_name").alias("nganh_hang"),
        F.col("price").alias("gia_Rp"),
        F.col("tong_luot_review"),
    )
)
top_products.show(truncate=50)

# ──────────────────────────────────────────────────────────────────────────────
# Phân tích 4 — Top thương hiệu bán nhiều nhất
# ──────────────────────────────────────────────────────────────────────────────
print("=" * 65)
print("  PHÂN TÍCH 4 — Top 10 thương hiệu / shop bán nhiều nhất")
print("=" * 65)

top_brands = (
    df_items
    .groupBy("brandName")
    .agg(
        F.sum("totalReviews").alias("tong_luot_review"),
        F.count("*").alias("so_san_pham"),
        F.round(F.avg("price"), 0).alias("gia_tb_Rp"),
    )
    .orderBy(F.desc("tong_luot_review"))
    .limit(10)
)
top_brands.show(truncate=False)

# ──────────────────────────────────────────────────────────────────────────────
# Phân tích 5 — Phân bố đánh giá sao
# ──────────────────────────────────────────────────────────────────────────────
print("=" * 65)
print("  PHÂN TÍCH 5 — Phân bố đánh giá sao")
print("=" * 65)

# --- Lazada ---
laz_total = df_reviews.count()
laz_rating_dist = (
    df_reviews
    .filter(F.col("rating").between(1, 5))
    .groupBy("rating")
    .agg(
        F.count("*").alias("so_danh_gia"),
        F.round(F.count("*") / laz_total * 100, 2).alias("phan_tram"),
    )
    .orderBy("rating")
)
print("  Lazada:")
laz_rating_dist.show()

# --- Shopee ---
sp_total = df_shopee.count()
sp_rating_dist = (
    df_shopee
    .filter(F.col("rating").between(1, 5))
    .groupBy("rating")
    .agg(
        F.count("*").alias("so_danh_gia"),
        F.round(F.count("*") / sp_total * 100, 2).alias("phan_tram"),
    )
    .orderBy("rating")
)
print("  Shopee:")
sp_rating_dist.show()

# --- Tổng hợp ---
laz_tagged = df_reviews.filter(F.col("rating").between(1, 5)).withColumn("source", F.lit("Lazada"))
sp_tagged  = df_shopee.filter(F.col("rating").between(1, 5)).withColumn("source", F.lit("Shopee"))

combined = (
    laz_tagged.select("rating", "source")
    .union(sp_tagged.select("rating", "source"))
)

grand_total = combined.count()
combined_dist = (
    combined
    .groupBy("rating")
    .agg(
        F.count("*").alias("tong_so"),
        F.round(F.count("*") / grand_total * 100, 2).alias("phan_tram"),
    )
    .orderBy("rating")
)
print(f"  Tổng hợp (Lazada + Shopee = {grand_total:,} đánh giá):")
combined_dist.show()

# ──────────────────────────────────────────────────────────────────────────────
# Kết thúc
# ──────────────────────────────────────────────────────────────────────────────
spark.stop()
print("=" * 65)
print("  ✓ Hoàn thành toàn bộ phân tích PySpark")
print("=" * 65)
