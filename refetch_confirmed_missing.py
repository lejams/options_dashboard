#!/usr/bin/env python3

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

import pandas as pd


DEFAULT_REPO_ROOT = Path("/Users/ismailje/Documents/dashboard_macro/original_repo/macro_engine")
DEFAULT_AUDIT_DIR = Path("/Users/ismailje/Documents/dashboard_macro/data/vm_option_data_full_20260322/audit")
DEFAULT_INPUT = DEFAULT_AUDIT_DIR / "confirmed_missing_post_backfill_detailed.csv"
DEFAULT_OUTPUT_PREFIX = "confirmed_missing_refetch"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Refetch confirmed missing option files by regrouping impacted ticker/date pairs."
    )
    parser.add_argument("--input-csv", default=str(DEFAULT_INPUT))
    parser.add_argument("--audit-dir", default=str(DEFAULT_AUDIT_DIR))
    parser.add_argument("--repo-root", default=str(DEFAULT_REPO_ROOT))
    parser.add_argument("--fetch-script", default=None)
    parser.add_argument("--verify-script", default=None)
    parser.add_argument("--python-bin", default=sys.executable)
    parser.add_argument("--output-prefix", default=DEFAULT_OUTPUT_PREFIX)
    parser.add_argument("--chunk-size", type=int, default=25)
    parser.add_argument("--max-groups", type=int, default=None)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--tickers", default=None)

    return parser.parse_args()


def require_columns(df: pd.DataFrame, required: list[str]) -> None:
    missing = [column for column in required if column not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns in input CSV: {missing}")


def build_groups(df: pd.DataFrame) -> pd.DataFrame:
    grouped = (
        df.groupby(["ticker", "date"], dropna=False)
        .size()
        .reset_index(name="missing_file_count")
        .sort_values(["missing_file_count", "ticker", "date"], ascending=[False, True, True])
        .reset_index(drop=True)
    )
    grouped["date"] = pd.to_datetime(grouped["date"]).dt.strftime("%Y-%m-%d")
    return grouped


def chunk_dataframe(df: pd.DataFrame, chunk_size: int) -> list[pd.DataFrame]:
    if chunk_size <= 0:
        raise ValueError("chunk-size must be > 0")
    return [df.iloc[start : start + chunk_size].copy() for start in range(0, len(df), chunk_size)]


def run_command(cmd: list[str], cwd: Path) -> None:
    print(" ".join(cmd))
    subprocess.run(cmd, cwd=str(cwd), check=True)


def summarize_verification(audit_dir: Path, prefix: str) -> tuple[Path, pd.DataFrame]:
    candidates = sorted(audit_dir.glob("fetch_audit_summary_*.csv"), key=lambda p: p.stat().st_mtime)
    if not candidates:
        raise FileNotFoundError(f"No fetch audit summary found in {audit_dir}")
    latest = candidates[-1]
    summary = pd.read_csv(latest)
    out_path = audit_dir / f"{prefix}_latest_verify_snapshot.csv"
    summary.to_csv(out_path, index=False)
    return out_path, summary


def write_outputs(
    groups: pd.DataFrame,
    attempted_groups: pd.DataFrame,
    audit_dir: Path,
    output_prefix: str,
) -> tuple[Path, Path]:
    groups_path = audit_dir / f"{output_prefix}_groups.csv"
    attempted_path = audit_dir / f"{output_prefix}_attempted_groups.csv"
    groups.to_csv(groups_path, index=False)
    attempted_groups.to_csv(attempted_path, index=False)
    return groups_path, attempted_path


def main() -> None:
    args = parse_args()
    repo_root = Path(args.repo_root)
    audit_dir = Path(args.audit_dir)
    input_csv = Path(args.input_csv)
    fetch_script = Path(args.fetch_script) if args.fetch_script else repo_root / "options_app" / "fetch_option_data.py"
    verify_script = Path(args.verify_script) if args.verify_script else repo_root / "options_app" / "verify_option_fetch.py"

    missing_df = pd.read_csv(input_csv)
    require_columns(missing_df, ["ticker", "date"])
    if args.tickers:
        requested = {ticker.strip() for ticker in args.tickers.split(",") if ticker.strip()}
        missing_df = missing_df[missing_df["ticker"].isin(requested)].copy()

    groups = build_groups(missing_df)
    if args.max_groups is not None:
        groups = groups.head(args.max_groups).copy()

    attempted_groups = groups.copy()
    groups_path, attempted_path = write_outputs(groups, attempted_groups, audit_dir, args.output_prefix)

    print(f"Input missing rows: {len(missing_df)}")
    print(f"Ticker/date groups to refetch: {len(groups)}")
    print(f"Grouped workload: {groups_path}")
    print(f"Attempt list: {attempted_path}")

    if groups.empty:
        print("No confirmed missing ticker/date groups to refetch.")
        return

    chunks = chunk_dataframe(groups, args.chunk_size)
    print(f"Refetch chunks: {len(chunks)} (chunk-size={args.chunk_size})")

    if args.dry_run:
        for idx, chunk in enumerate(chunks, start=1):
            tickers = ",".join(sorted(chunk["ticker"].unique()))
            start_date = chunk["date"].min()
            end_date = chunk["date"].max()
            print(
                f"Dry run chunk {idx}/{len(chunks)}: "
                f"{start_date} -> {end_date} | tickers={tickers} | groups={len(chunk)}"
            )
        return

    for idx, chunk in enumerate(chunks, start=1):
        tickers = sorted(chunk["ticker"].unique())
        start_date = chunk["date"].min()
        end_date = chunk["date"].max()

        print("")
        print(
            f"=== Refetch chunk {idx}/{len(chunks)}: "
            f"{start_date} -> {end_date} | {len(tickers)} ticker(s) | {len(chunk)} ticker/date groups ==="
        )

        fetch_cmd = [
            args.python_bin,
            str(fetch_script),
            "--start-date",
            start_date,
            "--end-date",
            end_date,
            "--tickers",
            ",".join(tickers),
        ]
        run_command(fetch_cmd, cwd=repo_root)

        verify_cmd = [
            args.python_bin,
            str(verify_script),
            "--start-date",
            start_date,
            "--end-date",
            end_date,
            "--tickers",
            ",".join(tickers),
            "--audit-dir",
            str(audit_dir),
        ]
        run_command(verify_cmd, cwd=repo_root)

    snapshot_path, verify_summary = summarize_verification(audit_dir, args.output_prefix)
    print("")
    print(f"Latest verification snapshot: {snapshot_path}")
    if not verify_summary.empty:
        first = verify_summary.iloc[0].to_dict()
        print("Verification summary:")
        for key, value in first.items():
            print(f"  {key}: {value}")


if __name__ == "__main__":
    main()
