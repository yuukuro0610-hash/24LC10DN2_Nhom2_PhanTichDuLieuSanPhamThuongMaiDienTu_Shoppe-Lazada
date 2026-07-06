"""
MapReduce 4: Top shop / thương hiệu bán được nhiều nhất
Input : cleaned_lazada_items.csv
Output: Top N brandName theo tổng totalReviews
"""

import csv
from collections import defaultdict

DATA_PATH = "cleaned_lazada_items.csv"
TOP_N     = 10

# ─── MAPPER ───────────────────────────────────────────────────────────────────
def mapper(filepath):
    """
    Phát ra cặp (brandName, totalReviews) cho mỗi sản phẩm.
    """
    pairs = []
    with open(filepath, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                brand   = row["brandName"].strip() or "Unknown"
                reviews = int(row["totalReviews"])
                price   = float(row["price"])
                pairs.append((brand, {"reviews": reviews, "price": price}))
            except (ValueError, KeyError):
                continue
    return pairs


# ─── SHUFFLER ─────────────────────────────────────────────────────────────────
def shuffler(mapped_pairs):
    """
    Nhóm tất cả bản ghi theo thương hiệu.
    """
    grouped = defaultdict(list)
    for key, value in mapped_pairs:
        grouped[key].append(value)
    return grouped


# ─── REDUCER ──────────────────────────────────────────────────────────────────
def reducer(grouped):
    """
    Tổng hợp theo thương hiệu:
      - total_reviews : tổng lượt đánh giá (proxy doanh số)
      - product_count : số dòng sản phẩm
      - avg_price     : giá trung bình
    """
    result = {}
    for brand, records in grouped.items():
        total_reviews = sum(r["reviews"] for r in records)
        avg_price     = sum(r["price"]   for r in records) / len(records)
        result[brand] = {
            "total_reviews": total_reviews,
            "product_count": len(records),
            "avg_price":     avg_price,
        }
    return result


# ─── MAIN ─────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 75)
    print(f"  MapReduce 4 — Top {TOP_N} thương hiệu / shop bán được nhiều nhất")
    print("=" * 75)

    mapped  = mapper(DATA_PATH)
    grouped = shuffler(mapped)
    result  = reducer(grouped)

    top_n = sorted(result.items(),
                   key=lambda x: x[1]["total_reviews"], reverse=True)[:TOP_N]

    print(f"\n{'#':<4} {'Thương hiệu':<18} {'Tổng Reviews':>14} {'Số SP':>7} {'Giá TB (Rp)':>16}")
    print("-" * 62)
    for rank, (brand, stats) in enumerate(top_n, 1):
        print(
            f"{rank:<4} {brand:<18} "
            f"{stats['total_reviews']:>14,} "
            f"{stats['product_count']:>7,} "
            f"{stats['avg_price']:>16,.0f}"
        )
