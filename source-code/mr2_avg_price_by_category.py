"""
MapReduce 2: Giá trung bình theo ngành hàng
Input : cleaned_lazada_items.csv
Output: category -> avg_price (Rupiah)
"""

import csv
from collections import defaultdict

DATA_PATH = "cleaned_lazada_items.csv"

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
    Phát ra cặp (category, price) cho mỗi sản phẩm.
    Bỏ qua các dòng thiếu giá hoặc giá không hợp lệ.
    """
    pairs = []
    with open(filepath, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                price = float(row["price"])
                if price <= 0:
                    continue
                category = CATEGORY_MAP.get(row["category"], row["category"])
                pairs.append((category, price))
            except (ValueError, KeyError):
                continue
    return pairs


# ─── SHUFFLER ─────────────────────────────────────────────────────────────────
def shuffler(mapped_pairs):
    """
    Nhóm danh sách giá theo từng ngành hàng.
    """
    grouped = defaultdict(list)
    for key, value in mapped_pairs:
        grouped[key].append(value)
    return grouped


# ─── REDUCER ──────────────────────────────────────────────────────────────────
def reducer(grouped):
    """
    Tính giá trung bình = tổng giá / số sản phẩm cho mỗi ngành.
    """
    result = {}
    for category, prices in grouped.items():
        result[category] = {
            "avg_price": sum(prices) / len(prices),
            "min_price": min(prices),
            "max_price": max(prices),
            "count":     len(prices),
        }
    return result


# ─── MAIN ─────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 70)
    print("  MapReduce 2 — Giá trung bình theo ngành hàng")
    print("=" * 70)

    mapped  = mapper(DATA_PATH)
    grouped = shuffler(mapped)
    result  = reducer(grouped)

    sorted_result = sorted(result.items(),
                           key=lambda x: x[1]["avg_price"], reverse=True)

    print(f"\n{'Ngành hàng':<18} {'Giá TB (Rp)':>16} {'Giá Min':>14} {'Giá Max':>16} {'SL':>6}")
    print("-" * 72)
    for cat, stats in sorted_result:
        print(
            f"{cat:<18} "
            f"{stats['avg_price']:>16,.0f} "
            f"{stats['min_price']:>14,.0f} "
            f"{stats['max_price']:>16,.0f} "
            f"{stats['count']:>6,}"
        )
