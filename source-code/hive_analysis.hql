-- ============================================================
-- HiveQL — Phân tích dữ liệu Shopee / Lazada
-- Bao gồm: tạo bảng, load dữ liệu, 5 câu truy vấn phân tích
-- Chạy: hive -f hive_analysis.hql
--        hoặc từng lệnh trong Hive CLI / Beeline
-- ============================================================


-- ────────────────────────────────────────────────────────────
-- 0. CHUẨN BỊ DATABASE
-- ────────────────────────────────────────────────────────────
CREATE DATABASE IF NOT EXISTS ecommerce;
USE ecommerce;


-- ────────────────────────────────────────────────────────────
-- TẠO BẢNG: lazada_items
-- ────────────────────────────────────────────────────────────
DROP TABLE IF EXISTS lazada_items;

CREATE TABLE lazada_items (
    itemId        BIGINT,
    category      STRING,
    name          STRING,
    brandName     STRING,
    url           STRING,
    price         DOUBLE,
    averageRating INT,
    totalReviews  INT,
    retrievedDate STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
TBLPROPERTIES ("skip.header.line.count"="1");

LOAD DATA LOCAL INPATH '/path/to/cleaned_lazada_items.csv'
OVERWRITE INTO TABLE lazada_items;


-- ────────────────────────────────────────────────────────────
-- TẠO BẢNG: lazada_reviews
-- ────────────────────────────────────────────────────────────
DROP TABLE IF EXISTS lazada_reviews;

CREATE TABLE lazada_reviews (
    itemId         BIGINT,
    category       STRING,
    name           STRING,
    rating         INT,
    originalRating STRING,
    reviewTitle    STRING,
    reviewContent  STRING,
    likeCount      INT,
    upVotes        INT,
    downVotes      INT,
    helpful        STRING,
    relevanceScore DOUBLE,
    boughtDate     STRING,
    clientType     STRING,
    retrievedDate  STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
TBLPROPERTIES ("skip.header.line.count"="1");

LOAD DATA LOCAL INPATH '/path/to/cleaned_lazada_reviews.csv'
OVERWRITE INTO TABLE lazada_reviews;


-- ────────────────────────────────────────────────────────────
-- TẠO BẢNG: shopee_reviews
-- ────────────────────────────────────────────────────────────
DROP TABLE IF EXISTS shopee_reviews;

CREATE TABLE shopee_reviews (
    review_id  BIGINT,
    review     STRING,
    rating     INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
TBLPROPERTIES ("skip.header.line.count"="1");

LOAD DATA LOCAL INPATH '/path/to/cleaned_shopee.csv'
OVERWRITE INTO TABLE shopee_reviews;


-- ────────────────────────────────────────────────────────────
-- VIEW: tên ngành hàng thân thiện (thay slug → tiếng Anh)
-- ────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW v_items AS
SELECT
    itemId,
    CASE category
        WHEN 'beli-harddisk-eksternal' THEN 'External HDD'
        WHEN 'jual-flash-drives'       THEN 'Flash Drives'
        WHEN 'beli-smart-tv'           THEN 'Smart TV'
        WHEN 'shop-televisi-digital'   THEN 'Digital TV'
        WHEN 'beli-laptop'             THEN 'Laptop'
        ELSE category
    END AS category_name,
    name,
    brandName,
    price,
    averageRating,
    totalReviews
FROM lazada_items;


-- ============================================================
-- PHÂN TÍCH 1 — Đếm số sản phẩm theo ngành hàng
-- ============================================================
SELECT
    category_name,
    COUNT(*)                                        AS so_san_pham,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS phan_tram
FROM v_items
GROUP BY category_name
ORDER BY so_san_pham DESC;


-- ============================================================
-- PHÂN TÍCH 2 — Giá trung bình theo ngành hàng
-- ============================================================
SELECT
    category_name,
    ROUND(AVG(price), 0)  AS gia_trung_binh_Rp,
    MIN(price)            AS gia_thap_nhat_Rp,
    MAX(price)            AS gia_cao_nhat_Rp,
    COUNT(*)              AS so_san_pham
FROM v_items
WHERE price > 0
GROUP BY category_name
ORDER BY gia_trung_binh_Rp DESC;


-- ============================================================
-- PHÂN TÍCH 3 — Top 10 sản phẩm bán chạy nhất
-- ============================================================
SELECT
    name,
    category_name,
    price            AS gia_Rp,
    MAX(totalReviews) AS tong_luot_review
FROM v_items
GROUP BY name, category_name, price
ORDER BY tong_luot_review DESC
LIMIT 10;


-- ============================================================
-- PHÂN TÍCH 4 — Top 10 thương hiệu / shop bán nhiều nhất
-- ============================================================
SELECT
    brandName,
    SUM(totalReviews)     AS tong_luot_review,
    COUNT(*)              AS so_san_pham,
    ROUND(AVG(price), 0)  AS gia_tb_Rp
FROM v_items
GROUP BY brandName
ORDER BY tong_luot_review DESC
LIMIT 10;


-- ============================================================
-- PHÂN TÍCH 5 — Phân bố đánh giá sao
-- ============================================================

-- 5a. Lazada
SELECT
    rating,
    COUNT(*)                                            AS so_danh_gia,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2)  AS phan_tram,
    CONCAT(REPEAT('█', CAST(COUNT(*)*30/SUM(COUNT(*)) OVER() AS INT)), '') AS bieu_do
FROM lazada_reviews
WHERE rating BETWEEN 1 AND 5
GROUP BY rating
ORDER BY rating;

-- 5b. Shopee
SELECT
    rating,
    COUNT(*)                                            AS so_danh_gia,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2)  AS phan_tram,
    CONCAT(REPEAT('█', CAST(COUNT(*)*30/SUM(COUNT(*)) OVER() AS INT)), '') AS bieu_do
FROM shopee_reviews
WHERE rating BETWEEN 1 AND 5
GROUP BY rating
ORDER BY rating;

-- 5c. Tổng hợp Lazada + Shopee
SELECT
    rating,
    SUM(so_danh_gia)                                             AS tong_so,
    ROUND(SUM(so_danh_gia) * 100.0 / SUM(SUM(so_danh_gia)) OVER(), 2) AS phan_tram
FROM (
    SELECT rating, COUNT(*) AS so_danh_gia FROM lazada_reviews
    WHERE rating BETWEEN 1 AND 5
    GROUP BY rating

    UNION ALL

    SELECT rating, COUNT(*) AS so_danh_gia FROM shopee_reviews
    WHERE rating BETWEEN 1 AND 5
    GROUP BY rating
) combined
GROUP BY rating
ORDER BY rating;
