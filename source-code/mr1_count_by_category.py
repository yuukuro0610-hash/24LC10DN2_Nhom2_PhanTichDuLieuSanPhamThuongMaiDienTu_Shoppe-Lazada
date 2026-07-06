"""
MapReduce 1: Đếm số sản phẩm theo ngành hàng
Input : cleaned_lazada_items.csv
Output: category -> count
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
    Đọc từng dòng CSV, phát ra cặp (category, 1).
    """
    pairs = []
    with open(filepath, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            category = CATEGORY_MAP.get(row["category"], row["category"])
            pairs.append((category, 1))
    return pairs


# ─── SHUFFLER ─────────────────────────────────────────────────────────────────
def shuffler(mapped_pairs):
    """
    Nhóm tất cả giá trị theo key.
    """
    grouped = defaultdict(list)
    for key, value in mapped_pairs:
        grouped[key].append(value)
    return grouped


# ─── REDUCER ──────────────────────────────────────────────────────────────────
def reducer(grouped):
    """
    Tổng hợp: cộng tất cả giá trị 1 lại → số sản phẩm mỗi ngành.
    """
    result = {}
    for category, values in grouped.items():
        result[category] = sum(values)
    return result


# ─── MAIN ─────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 55)
    print("  MapReduce 1 — Đếm số sản phẩm theo ngành hàng")
    print("=" * 55)

    mapped   = mapper(DATA_PATH)
    grouped  = shuffler(mapped)
    result   = reducer(grouped)

    sorted_result = sorted(result.items(), key=lambda x: x[1], reverse=True)

    print(f"\n{'Ngành hàng':<20} {'Số sản phẩm':>12}")
    print("-" * 34)
    total = 0
    for cat, cnt in sorted_result:
        print(f"{cat:<20} {cnt:>12,}")
        total += cnt
    print("-" * 34)
    print(f"{'TỔNG CỘNG':<20} {total:>12,}")
