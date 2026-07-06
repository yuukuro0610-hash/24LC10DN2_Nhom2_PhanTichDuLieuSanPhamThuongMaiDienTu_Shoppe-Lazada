"""
MapReduce 3: Top sản phẩm bán chạy
Input : cleaned_lazada_items.csv
Output: Top N sản phẩm theo totalReviews (proxy doanh số)
"""

import csv
from collections import defaultdict

DATA_PATH = "cleaned_lazada_items.csv"
TOP_N     = 10

CATEGORY_MAP = {
    "beli-harddisk-eksternal": "External HDD",
    "jual-flash-drives":       "Flash Drives",
    "beli-smart-tv":           "Smart TV",
    "shop-televisi-digital":   "Digital TV",
    "beli-laptop":             "Laptop",
}

# ─── MAPPER ───────────────────────────────────────────────────────────────────
def mapper(filepath):
    """
    Phát ra cặp (product_name, {totalReviews, price, category}).
    Dùng tên sản phẩm làm key để gom các bản ghi trùng.
    """
    pairs = []
    with open(filepath, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                name     = row["name"].strip()
                reviews  = int(row["totalReviews"])
                price    = float(row["price"])
                category = CATEGORY_MAP.get(row["category"], row["category"])
                pairs.append((name, {"reviews": reviews,
                                     "price":   price,
                                     "category": category}))
            except (ValueError, KeyError):
                continue
    return pairs


# ─── SHUFFLER ─────────────────────────────────────────────────────────────────
def shuffler(mapped_pairs):
    """
    Nhóm tất cả bản ghi theo tên sản phẩm.
    """
    grouped = defaultdict(list)
    for key, value in mapped_pairs:
        grouped[key].append(value)
    return grouped


# ─── REDUCER ──────────────────────────────────────────────────────────────────
def reducer(grouped):
    """
    Với mỗi sản phẩm: lấy totalReviews max (có thể xuất hiện nhiều lần
    do trùng lặp slug), giữ giá và category tương ứng.
    """
    result = {}
    for name, records in grouped.items():
        best = max(records, key=lambda r: r["reviews"])
        result[name] = best
    return result


# ─── MAIN ─────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 75)
    print(f"  MapReduce 3 — Top {TOP_N} sản phẩm bán chạy nhất (theo lượt review)")
    print("=" * 75)

    mapped  = mapper(DATA_PATH)
    grouped = shuffler(mapped)
    result  = reducer(grouped)

    top_n = sorted(result.items(),
                   key=lambda x: x[1]["reviews"], reverse=True)[:TOP_N]

    print(f"\n{'#':<4} {'Tên sản phẩm':<48} {'Ngành':>12} {'Reviews':>9} {'Giá (Rp)':>14}")
    print("-" * 90)
    for rank, (name, stats) in enumerate(top_n, 1):
        short_name = (name[:45] + "...") if len(name) > 45 else name
        print(
            f"{rank:<4} {short_name:<48} "
            f"{stats['category']:>12} "
            f"{stats['reviews']:>9,} "
            f"{stats['price']:>14,.0f}"
        )
