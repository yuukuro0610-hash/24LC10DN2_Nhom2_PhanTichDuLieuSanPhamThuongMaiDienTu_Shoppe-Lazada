"""
MapReduce 5: Phân bố đánh giá sao
Input : cleaned_lazada_reviews.csv  +  cleaned_shopee.csv
Output: rating (1–5) -> count, percentage  (cả hai nguồn)
"""

import csv
from collections import defaultdict

LAZADA_PATH = "cleaned_lazada_reviews.csv"
SHOPEE_PATH = "cleaned_shopee.csv"

# ─── MAPPER ───────────────────────────────────────────────────────────────────
def mapper(filepath, rating_col="rating"):
    """
    Đọc file CSV, phát ra cặp (rating, 1) cho từng đánh giá hợp lệ.
    """
    pairs = []
    with open(filepath, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                rating = int(float(row[rating_col]))
                if 1 <= rating <= 5:
                    pairs.append((rating, 1))
            except (ValueError, KeyError):
                continue
    return pairs


# ─── SHUFFLER ─────────────────────────────────────────────────────────────────
def shuffler(mapped_pairs):
    """
    Nhóm danh sách 1 theo từng mức sao.
    """
    grouped = defaultdict(list)
    for key, value in mapped_pairs:
        grouped[key].append(value)
    return grouped


# ─── REDUCER ──────────────────────────────────────────────────────────────────
def reducer(grouped):
    """
    Tính số lượng và tỷ lệ phần trăm cho từng mức sao.
    """
    counts = {k: sum(v) for k, v in grouped.items()}
    total  = sum(counts.values())
    result = {}
    for star in range(1, 6):
        cnt = counts.get(star, 0)
        result[star] = {
            "count":      cnt,
            "percentage": (cnt / total * 100) if total > 0 else 0,
        }
    return result, total


# ─── PRINT HELPER ─────────────────────────────────────────────────────────────
def print_result(label, result, total):
    BAR_MAX = 30
    print(f"\n  {label}  (tổng: {total:,} đánh giá)")
    print(f"  {'Sao':<6} {'Số lượng':>10} {'%':>7}  {'Biểu đồ'}")
    print("  " + "-" * 58)
    for star in range(5, 0, -1):
        stats = result[star]
        bar   = "█" * int(stats["percentage"] / 100 * BAR_MAX)
        print(
            f"  {star} ★   "
            f"{stats['count']:>10,}  "
            f"{stats['percentage']:>6.1f}%  "
            f"{bar}"
        )


# ─── MAIN ─────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 62)
    print("  MapReduce 5 — Phân bố đánh giá sao")
    print("=" * 62)

    # --- Lazada ---
    laz_mapped  = mapper(LAZADA_PATH, rating_col="rating")
    laz_grouped = shuffler(laz_mapped)
    laz_result, laz_total = reducer(laz_grouped)
    print_result("LAZADA", laz_result, laz_total)

    # --- Shopee ---
    sp_mapped   = mapper(SHOPEE_PATH, rating_col="rating")
    sp_grouped  = shuffler(sp_mapped)
    sp_result, sp_total = reducer(sp_grouped)
    print_result("SHOPEE", sp_result, sp_total)

    # --- Tổng hợp cả hai ---
    print(f"\n  TỔNG HỢP  (Lazada + Shopee)")
    print(f"  {'Sao':<6} {'Lazada':>10} {'Shopee':>10} {'Tổng':>10} {'%':>7}")
    print("  " + "-" * 48)
    grand_total = laz_total + sp_total
    for star in range(5, 0, -1):
        l_cnt = laz_result[star]["count"]
        s_cnt = sp_result[star]["count"]
        total = l_cnt + s_cnt
        pct   = total / grand_total * 100 if grand_total > 0 else 0
        print(
            f"  {star} ★   "
            f"{l_cnt:>10,} "
            f"{s_cnt:>10,} "
            f"{total:>10,} "
            f"{pct:>6.1f}%"
        )
    print(f"\n  Grand total: {grand_total:,} đánh giá")
