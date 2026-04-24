import argparse
import csv
import os
from collections import Counter, defaultdict

DEFAULT_AUDIT_DIR = "/Users/ismailje/Documents/dashboard_macro/data/vm_option_data_full_20260322/audit"
DEFAULT_REVALIDATED = os.path.join(DEFAULT_AUDIT_DIR, "problematic_post_backfill_revalidated.csv")


def parse_args():
    parser = argparse.ArgumentParser(description="Inspect revalidated problematic option files.")
    parser.add_argument("--revalidated-path", default=DEFAULT_REVALIDATED)
    parser.add_argument("--top-n", type=int, default=20)
    parser.add_argument("--examples-per-group", type=int, default=3)
    return parser.parse_args()


def read_csv_rows(path):
    with open(path, newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def print_counter(title, counter, top_n):
    print(f"\n{title}")
    for key, count in counter.most_common(top_n):
        print(f"  {key}: {count}")


def print_examples(title, rows, group_fields, examples_per_group, top_n):
    grouped = defaultdict(list)
    for row in rows:
        key = tuple(row.get(field, "") for field in group_fields)
        grouped[key].append(row)

    ranked = sorted(grouped.items(), key=lambda item: len(item[1]), reverse=True)[:top_n]
    print(f"\n{title}")
    for key, items in ranked:
        label = ", ".join(f"{field}={value}" for field, value in zip(group_fields, key))
        print(f"  Group: {label} ({len(items)} file(s))")
        for row in items[:examples_per_group]:
            print(
                "    - "
                f"{row.get('ticker','')} {row.get('date','')} {row.get('sf','')} {row.get('cp','')} {row.get('metric','')} "
                f"status={row.get('current_status','')} class={row.get('revalidation_class','')} path={row.get('path','')}"
            )


def main():
    args = parse_args()
    rows = read_csv_rows(args.revalidated_path)

    class_counter = Counter(row.get("revalidation_class", "") for row in rows)
    status_counter = Counter(row.get("current_status", "") for row in rows)
    ticker_counter = Counter(row.get("ticker", "") for row in rows)
    type_counter = Counter(
        f"{row.get('sf','')}|{row.get('cp','')}|{row.get('metric','')}" for row in rows
    )

    confirmed_bad = [row for row in rows if row.get("revalidation_class") == "confirmed_bad"]
    confirmed_missing = [row for row in rows if row.get("revalidation_class") == "confirmed_missing"]
    recovered_ok = [row for row in rows if row.get("revalidation_class") == "recovered_ok"]

    print(f"Loaded: {args.revalidated_path}")
    print(f"Total rows: {len(rows)}")

    print_counter("Revalidation Classes", class_counter, args.top_n)
    print_counter("Current Statuses", status_counter, args.top_n)
    print_counter("Top Tickers", ticker_counter, args.top_n)
    print_counter("Top Types (sf|cp|metric)", type_counter, args.top_n)

    print_examples(
        "Examples: Confirmed Bad by Ticker",
        confirmed_bad,
        ["ticker"],
        args.examples_per_group,
        min(args.top_n, 10),
    )
    print_examples(
        "Examples: Confirmed Bad by Type",
        confirmed_bad,
        ["sf", "cp", "metric"],
        args.examples_per_group,
        min(args.top_n, 10),
    )
    print_examples(
        "Examples: Confirmed Missing by Ticker",
        confirmed_missing,
        ["ticker"],
        args.examples_per_group,
        min(args.top_n, 10),
    )
    print_examples(
        "Examples: Recovered OK by Ticker",
        recovered_ok,
        ["ticker"],
        args.examples_per_group,
        min(args.top_n, 10),
    )


if __name__ == "__main__":
    main()
