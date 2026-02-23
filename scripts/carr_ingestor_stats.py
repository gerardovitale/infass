from __future__ import annotations

import argparse
import csv
from collections import Counter
from collections import defaultdict
from dataclasses import dataclass
from dataclasses import field
from pathlib import Path
from typing import DefaultDict
from typing import Iterable


MISSING = "(missing)"
NO_SUBCATEGORY = "(none)"


@dataclass
class Stats:
    total_records: int = 0
    unique_products: set[str] = field(default_factory=set)
    records_by_category: Counter[str] = field(default_factory=Counter)
    products_by_category: DefaultDict[str, set[str]] = field(default_factory=lambda: defaultdict(set))
    records_by_cat_sub: Counter[tuple[str, str]] = field(default_factory=Counter)
    products_by_cat_sub: DefaultDict[tuple[str, str], set[str]] = field(default_factory=lambda: defaultdict(set))

    def add(self, category: str, subcategory: str, product_key: str) -> None:
        self.total_records += 1
        self.unique_products.add(product_key)

        self.records_by_category[category] += 1
        self.products_by_category[category].add(product_key)

        cat_sub_key = (category, subcategory)
        self.records_by_cat_sub[cat_sub_key] += 1
        self.products_by_cat_sub[cat_sub_key].add(product_key)

    def category_rows(self) -> list[tuple[str, int, int]]:
        rows = [
            (category, count, len(self.products_by_category[category]))
            for category, count in self.records_by_category.items()
        ]
        rows.sort(key=lambda item: (-item[1], item[0]))
        return rows

    def cat_sub_rows(self) -> list[tuple[str, str, int, int]]:
        rows = [
            (category, subcategory, count, len(self.products_by_cat_sub[(category, subcategory)]))
            for (category, subcategory), count in self.records_by_cat_sub.items()
        ]
        rows.sort(key=lambda item: (-item[2], item[0], item[1]))
        return rows


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Calculate basic monitoring stats for carr CSV output files.")
    parser.add_argument(
        "--glob",
        default="data/ingestor_output/carr_*.csv",
        help="Glob pattern used to discover CSV files.",
    )
    parser.add_argument(
        "--top",
        type=int,
        default=0,
        help="Limit rows shown in category tables (0 means show all).",
    )
    return parser.parse_args()


def parse_category(value: str | None) -> tuple[str, str]:
    raw = (value or "").strip()
    if not raw:
        return MISSING, MISSING

    parts = [part.strip() for part in raw.split(">") if part.strip()]
    if not parts:
        return MISSING, MISSING
    if len(parts) == 1:
        return parts[0], NO_SUBCATEGORY
    return parts[0], " > ".join(parts[1:])


def normalize_name(value: str | None) -> str:
    return " ".join((value or "").split()).lower()


def product_key_for_row(row: dict[str, str]) -> str:
    product_url = (row.get("product_url") or "").strip()
    if product_url:
        return f"url::{product_url}"

    name = normalize_name(row.get("name"))
    if name:
        return f"name::{name}"
    return "unknown"


def load_stats_for_file(path: Path) -> Stats:
    stats = Stats()

    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            category, subcategory = parse_category(row.get("category"))
            stats.add(category=category, subcategory=subcategory, product_key=product_key_for_row(row))

    return stats


def render_table(headers: list[str], rows: Iterable[tuple], top: int = 0) -> str:
    materialized_rows = list(rows)
    if top and top > 0:
        materialized_rows = materialized_rows[:top]

    if not materialized_rows:
        return "(no data)"

    widths = [len(h) for h in headers]
    for row in materialized_rows:
        for idx, value in enumerate(row):
            widths[idx] = max(widths[idx], len(str(value)))

    def format_row(values: tuple | list[str]) -> str:
        return " | ".join(str(value).ljust(widths[idx]) for idx, value in enumerate(values))

    out = [format_row(headers), "-+-".join("-" * w for w in widths)]
    out.extend(format_row(row) for row in materialized_rows)
    return "\n".join(out)


def print_stats_block(label: str, stats: Stats, top: int) -> None:
    print("\n\n================================================================================")
    print("================================================================================")
    print(f"=== {label} ===")
    print(f"total_records: {stats.total_records}")
    print(f"unique_products_total: {len(stats.unique_products)}")

    print("\nrecords_and_unique_products_by_category")
    print(render_table(["category", "records", "unique_products"], stats.category_rows(), top=top))

    print("\nrecords_and_unique_products_by_category_subcategory")
    print(
        render_table(
            ["category", "subcategory", "records", "unique_products"],
            stats.cat_sub_rows(),
            top=top,
        )
    )


def main() -> None:
    args = parse_args()

    paths = sorted(Path().glob(args.glob))
    if not paths:
        raise SystemExit(f"No files matched pattern: {args.glob}")

    overall = Stats()

    for path in paths:
        per_file = load_stats_for_file(path)
        print_stats_block(path.name, per_file, args.top)

        overall.total_records += per_file.total_records
        overall.unique_products.update(per_file.unique_products)

        overall.records_by_category.update(per_file.records_by_category)
        for category, keys in per_file.products_by_category.items():
            overall.products_by_category[category].update(keys)

        overall.records_by_cat_sub.update(per_file.records_by_cat_sub)
        for cat_sub, keys in per_file.products_by_cat_sub.items():
            overall.products_by_cat_sub[cat_sub].update(keys)

    print_stats_block("OVERALL", overall, args.top)


if __name__ == "__main__":
    main()
