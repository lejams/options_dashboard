import argparse
import csv
import glob
import os
import signal
import subprocess
import sys
import time
from datetime import datetime

import pandas as pd

DEFAULT_REPO_ROOT = "/Users/ismailje/Documents/dashboard_macro/original_repo/macro_engine"
DEFAULT_FETCH_SCRIPT = os.path.join(DEFAULT_REPO_ROOT, "options_app", "fetch_option_data.py")
DEFAULT_VERIFY_SCRIPT = os.path.join(DEFAULT_REPO_ROOT, "options_app", "verify_option_fetch.py")
DEFAULT_AUDIT_DIR = "/Users/ismailje/Documents/dashboard_macro/data/vm_option_data_full_20260322/audit"
DEFAULT_PENDING_WEEKS_PATH = os.path.join(DEFAULT_AUDIT_DIR, "pending_weekly_backfill.csv")
DEFAULT_RAW_PATH = "/Users/ismailje/Documents/dashboard_macro/data/vm_option_data_full_20260322/raw_percent"
DEFAULT_TICKERS = [
    'SPX', 'QQQ US', 'DIA US', 'IWM US', 'GLD US', 'XLF US', 'XLE US', 'XLC US',
    'XLP US', 'XLV US', 'IYR US', 'HYG US', 'EEM US', 'FXI US', 'EWZ US', 'EWI US',
    'TLT UQ', 'XLK UP', 'XHB UP', 'SLV UP', 'USO UP', 'SX5E', 'NKY', 'NDX', 'RTY',
    'DAX', 'UKX', 'SMI', 'HSCEI', 'HSI', 'KOSPI2', 'AS51'
]


def parse_args():
    parser = argparse.ArgumentParser(description="Weekly options backfill with verification and retry.")
    parser.add_argument("--start-date", required=True, help="Backfill start date in YYYY-MM-DD format.")
    parser.add_argument("--end-date", required=True, help="Backfill end date in YYYY-MM-DD format.")
    parser.add_argument(
        "--chunk-weeks",
        type=int,
        default=1,
        help="Number of calendar weeks per backfill chunk. Default is 1.",
    )
    parser.add_argument(
        "--max-attempts",
        type=int,
        default=2,
        help="Max fetch attempts per chunk before stopping. Default is 2.",
    )
    parser.add_argument(
        "--tickers",
        default=",".join(DEFAULT_TICKERS),
        help="Comma-separated ticker list. Defaults to the full active universe.",
    )
    parser.add_argument("--repo-root", default=DEFAULT_REPO_ROOT)
    parser.add_argument("--fetch-script", default=DEFAULT_FETCH_SCRIPT)
    parser.add_argument("--verify-script", default=DEFAULT_VERIFY_SCRIPT)
    parser.add_argument("--audit-dir", default=DEFAULT_AUDIT_DIR)
    parser.add_argument("--pending-weeks-path", default=DEFAULT_PENDING_WEEKS_PATH)
    parser.add_argument("--fetch-command-retries", type=int, default=3, help="Automatic retries for transient fetch subprocess failures. Default is 3.")
    parser.add_argument("--raw-path", default=DEFAULT_RAW_PATH, help="Raw options folder to watch for file-write progress.")
    parser.add_argument("--stale-seconds", type=int, default=180, help="Kill fetch if no raw file changes are seen for this many seconds. Default is 180.")
    parser.add_argument("--max-chunk-seconds", type=int, default=1200, help="Hard runtime limit per fetch command in seconds. Default is 1200.")
    return parser.parse_args()


def business_dates(start_date: str, end_date: str):
    dates = pd.bdate_range(start=start_date, end=end_date)
    return [d.strftime("%Y-%m-%d") for d in dates]


def chunk_business_dates(all_dates, chunk_weeks):
    if not all_dates:
        return []
    chunk_size = max(chunk_weeks * 5, 1)
    return [all_dates[idx: idx + chunk_size] for idx in range(0, len(all_dates), chunk_size)]


def latest_file(pattern, created_after):
    candidates = [path for path in glob.glob(pattern) if os.path.getmtime(path) >= created_after]
    if not candidates:
        return None
    return max(candidates, key=os.path.getmtime)


def read_csv_rows(path):
    if not path or not os.path.exists(path):
        return []
    with open(path, newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def append_pending_week(path, row):
    fieldnames = [
        "logged_at",
        "chunk_start",
        "chunk_end",
        "attempts",
        "expected_count",
        "ok_count",
        "missing_count",
        "bad_count",
        "problematic_count",
        "problematic_pct",
        "impacted_tickers",
        "summary_path",
        "missing_path",
        "bad_path",
    ]
    file_exists = os.path.exists(path)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        if not file_exists:
            writer.writeheader()
        writer.writerow(row)


def run_command(cmd, env):
    print(" ".join(cmd), flush=True)
    subprocess.run(cmd, env=env, check=True)


def latest_raw_mtime(raw_path):
    latest = None
    if not os.path.exists(raw_path):
        return None
    for name in os.listdir(raw_path):
        file_path = os.path.join(raw_path, name)
        if not os.path.isfile(file_path):
            continue
        try:
            mtime = os.path.getmtime(file_path)
        except OSError:
            continue
        if latest is None or mtime > latest:
            latest = mtime
    return latest


def terminate_process(proc):
    if proc.poll() is not None:
        return
    proc.terminate()
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=5)


def run_fetch_once_with_watchdog(cmd, env, raw_path, stale_seconds, max_chunk_seconds):
    print(" ".join(cmd), flush=True)
    proc = subprocess.Popen(cmd, env=env)
    start_time = time.time()
    last_seen_mtime = latest_raw_mtime(raw_path)
    last_progress_time = start_time

    while True:
        return_code = proc.poll()
        if return_code is not None:
            if return_code == 0:
                return
            raise subprocess.CalledProcessError(return_code, cmd)

        now = time.time()
        current_mtime = latest_raw_mtime(raw_path)
        if current_mtime is not None and (last_seen_mtime is None or current_mtime > last_seen_mtime):
            last_seen_mtime = current_mtime
            last_progress_time = now

        runtime = now - start_time
        stale_for = now - last_progress_time

        if runtime > max_chunk_seconds:
            terminate_process(proc)
            raise subprocess.CalledProcessError(-signal.SIGTERM, cmd)

        if stale_for > stale_seconds:
            terminate_process(proc)
            raise subprocess.CalledProcessError(-signal.SIGTERM, cmd)

        time.sleep(5)


def run_fetch_command(cmd, env, retries, raw_path, stale_seconds, max_chunk_seconds):
    last_error = None
    for attempt in range(1, retries + 1):
        try:
            run_fetch_once_with_watchdog(cmd, env, raw_path, stale_seconds, max_chunk_seconds)
            return
        except subprocess.CalledProcessError as exc:
            last_error = exc
            if attempt == retries:
                raise
            wait_seconds = 5 * attempt
            print(
                f"Fetch command failed on attempt {attempt}/{retries} with exit code {exc.returncode}. Retrying in {wait_seconds}s...",
                flush=True,
            )
            time.sleep(wait_seconds)
    if last_error is not None:
        raise last_error


def verify_chunk(args, env, chunk_start, chunk_end, tickers_csv):
    started_at = datetime.now().timestamp()
    cmd = [
        sys.executable,
        args.verify_script,
        "--start-date",
        chunk_start,
        "--end-date",
        chunk_end,
        "--tickers",
        tickers_csv,
        "--audit-dir",
        args.audit_dir,
    ]
    run_command(cmd, env)
    summary_path = latest_file(os.path.join(args.audit_dir, "fetch_audit_summary_*.csv"), started_at)
    missing_path = latest_file(os.path.join(args.audit_dir, "missing_files_*.csv"), started_at)
    bad_path = latest_file(os.path.join(args.audit_dir, "bad_files_*.csv"), started_at)
    return summary_path, missing_path, bad_path


def count_summary(summary_path):
    rows = read_csv_rows(summary_path)
    return {row["metric"]: row["value"] for row in rows}


def impacted_tickers(missing_path, bad_path):
    tickers = set()
    for row in read_csv_rows(missing_path):
        if row.get("ticker"):
            tickers.add(row["ticker"])
    for row in read_csv_rows(bad_path):
        if row.get("ticker"):
            tickers.add(row["ticker"])
    return sorted(tickers)


def fetch_chunk(args, env, chunk_start, chunk_end, tickers_csv):
    cmd = [
        sys.executable,
        args.fetch_script,
        "--start-date",
        chunk_start,
        "--end-date",
        chunk_end,
        "--tickers",
        tickers_csv,
    ]
    run_fetch_command(
        cmd,
        env,
        args.fetch_command_retries,
        args.raw_path,
        args.stale_seconds,
        args.max_chunk_seconds,
    )


def main():
    args = parse_args()
    env = os.environ.copy()
    env["PYTHONPATH"] = args.repo_root

    all_tickers = [item.strip() for item in args.tickers.split(",") if item.strip()]
    all_dates = business_dates(args.start_date, args.end_date)
    chunks = chunk_business_dates(all_dates, args.chunk_weeks)

    if not chunks:
        raise SystemExit("No business dates found in the requested range.")

    print(
        f"Weekly backfill: {len(all_tickers)} tickers, {len(all_dates)} business dates, {len(chunks)} chunks, max {args.max_attempts} attempt(s) per chunk",
        flush=True,
    )

    for idx, chunk in enumerate(chunks, start=1):
        chunk_start = chunk[0]
        chunk_end = chunk[-1]
        retry_tickers = list(all_tickers)
        print(f"\n=== Chunk {idx}/{len(chunks)}: {chunk_start} -> {chunk_end} ({len(chunk)} business dates) ===", flush=True)

        for attempt in range(1, args.max_attempts + 1):
            tickers_csv = ",".join(retry_tickers)
            print(
                f"Attempt {attempt}/{args.max_attempts}: fetching {len(retry_tickers)} ticker(s) for {chunk_start} -> {chunk_end}",
                flush=True,
            )
            fetch_chunk(args, env, chunk_start, chunk_end, tickers_csv)
            summary_path, missing_path, bad_path = verify_chunk(args, env, chunk_start, chunk_end, tickers_csv)
            summary = count_summary(summary_path)
            missing_count = int(summary.get("missing_file_count", 0))
            bad_count = int(summary.get("bad_file_count", 0))
            ok_count = int(summary.get("ok_file_count", 0))
            expected_count = int(summary.get("expected_file_count", 0))
            problematic_count = missing_count + bad_count
            problematic_pct = (problematic_count / expected_count * 100.0) if expected_count else 0.0
            print(
                f"Verification: ok={ok_count}, missing={missing_count}, bad={bad_count}, expected={expected_count}",
                flush=True,
            )
            if problematic_count == 0:
                print(f"All files for {chunk_start} -> {chunk_end} are ok", flush=True)
                break

            print(
                f"Problematic files after attempt {attempt}: {problematic_count}/{expected_count} ({problematic_pct:.2f}%)",
                flush=True,
            )

            retry_tickers = impacted_tickers(missing_path, bad_path)
            if attempt == args.max_attempts:
                append_pending_week(
                    args.pending_weeks_path,
                    {
                        "logged_at": datetime.now().isoformat(timespec="seconds"),
                        "chunk_start": chunk_start,
                        "chunk_end": chunk_end,
                        "attempts": attempt,
                        "expected_count": expected_count,
                        "ok_count": ok_count,
                        "missing_count": missing_count,
                        "bad_count": bad_count,
                        "problematic_count": problematic_count,
                        "problematic_pct": f"{problematic_pct:.2f}",
                        "impacted_tickers": ",".join(retry_tickers),
                        "summary_path": summary_path or "",
                        "missing_path": missing_path or "",
                        "bad_path": bad_path or "",
                    },
                )
                print(
                    f"Chunk still has problems; saved to {args.pending_weeks_path}. Remaining impacted tickers: {', '.join(retry_tickers[:12])}" +
                    ("..." if len(retry_tickers) > 12 else ""),
                    flush=True,
                )
                break

            print(
                f"Retrying chunk {chunk_start} -> {chunk_end} for {len(retry_tickers)} impacted ticker(s)",
                flush=True,
            )

    print(f"\nBackfill finished through the requested end date. Pending weeks file: {args.pending_weeks_path}", flush=True)


if __name__ == "__main__":
    main()
