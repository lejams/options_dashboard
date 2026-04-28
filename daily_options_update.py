import argparse
import os
import re
import shutil
import subprocess
import sys
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path

from app import DATA_ROOT, tickers as DEFAULT_TICKERS


RAW_FILENAME_PATTERN = re.compile(
    r"^(?P<ticker>.+)_(?P<surface>spot|fwd)_(?P<option_type>Call|Put)_option_(?P<metric>percent|vol)_(?P<date>\d{4}-\d{2}-\d{2})\.csv$"
)

EXPECTED_FILE_KINDS = {
    (surface, option_type, metric)
    for surface in ("spot", "fwd")
    for option_type in ("Call", "Put")
    for metric in ("percent", "vol")
}


def parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Daily orchestration: fetch missing option CSVs, update master parquet, "
            "update percentile/strategies parquet, then sync data to AWS EC2."
        )
    )
    parser.add_argument(
        "--tickers",
        default=",".join(DEFAULT_TICKERS),
        help="Comma-separated tickers to update. Defaults to app.py tickers.",
    )
    parser.add_argument(
        "--end-date",
        default=date.today().strftime("%Y-%m-%d"),
        help="End date in YYYY-MM-DD format. Defaults to today.",
    )
    parser.add_argument(
        "--start-date",
        default=None,
        help=(
            "Optional explicit start date for all tickers. If omitted, each ticker starts "
            "from the first business day after its latest complete raw_percent date."
        ),
    )
    parser.add_argument(
        "--data-root",
        default=DATA_ROOT,
        help="Local options data root. Defaults to OPTIONS_DATA_ROOT/app.py DATA_ROOT.",
    )
    parser.add_argument(
        "--ssh-target",
        default=os.getenv("OPTIONS_AWS_SSH_TARGET", "ubuntu@13.61.178.147"),
        help="EC2 SSH target, e.g. ubuntu@13.61.178.147.",
    )
    parser.add_argument(
        "--ssh-key",
        default=os.getenv("OPTIONS_AWS_SSH_KEY", "/Users/ismailje/Downloads/options-dashboard-key.pem"),
        help="Path to EC2 .pem key.",
    )
    parser.add_argument(
        "--remote-data-dir",
        default=os.getenv("OPTIONS_AWS_DATA_DIR", "/opt/options_data"),
        help="Remote EC2 data directory.",
    )
    parser.add_argument(
        "--skip-fetch",
        action="store_true",
        help="Skip ICE fetch and only rebuild parquet/sync AWS.",
    )
    parser.add_argument(
        "--skip-master",
        action="store_true",
        help="Skip master parquet sync.",
    )
    parser.add_argument(
        "--skip-percentile",
        action="store_true",
        help="Skip percentile master sync.",
    )
    parser.add_argument(
        "--skip-strategies",
        action="store_true",
        help="Skip strategies master sync.",
    )
    parser.add_argument(
        "--skip-aws-sync",
        action="store_true",
        help="Skip rsync to EC2.",
    )
    parser.add_argument(
        "--no-delete-remote",
        action="store_true",
        help="Do not pass --delete to rsync.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print planned commands without running them.",
    )
    return parser.parse_args()


def parse_date(value):
    return datetime.strptime(value, "%Y-%m-%d").date()


def format_date(value):
    return value.strftime("%Y-%m-%d")


def is_business_day(value):
    return value.weekday() < 5


def next_business_day(value):
    current = value + timedelta(days=1)
    while not is_business_day(current):
        current += timedelta(days=1)
    return current


def business_dates(start, end):
    current = start
    out = []
    while current <= end:
        if is_business_day(current):
            out.append(current)
        current += timedelta(days=1)
    return out


def run_command(cmd, *, env=None, dry_run=False):
    printable = " ".join(str(part) for part in cmd)
    print(f"\n$ {printable}", flush=True)
    if dry_run:
        return
    subprocess.run(cmd, check=True, env=env)


def discover_latest_complete_dates(raw_dir, selected_tickers):
    complete_by_ticker_date = defaultdict(set)

    if not raw_dir.exists():
        return {}

    selected = set(selected_tickers)
    for path in raw_dir.iterdir():
        if not path.is_file():
            continue
        match = RAW_FILENAME_PATTERN.match(path.name)
        if not match:
            continue
        info = match.groupdict()
        ticker = info["ticker"]
        if ticker not in selected:
            continue
        kind = (info["surface"], info["option_type"], info["metric"])
        complete_by_ticker_date[(ticker, info["date"])].add(kind)

    latest = {}
    for ticker in selected_tickers:
        complete_dates = [
            parse_date(date_str)
            for (item_ticker, date_str), kinds in complete_by_ticker_date.items()
            if item_ticker == ticker and EXPECTED_FILE_KINDS.issubset(kinds)
        ]
        if complete_dates:
            latest[ticker] = max(complete_dates)
    return latest


def plan_fetch_groups(raw_dir, selected_tickers, explicit_start, end_date):
    if explicit_start is not None:
        start = parse_date(explicit_start)
        return {
            start: [ticker for ticker in selected_tickers if business_dates(start, end_date)]
        }

    latest = discover_latest_complete_dates(raw_dir, selected_tickers)
    groups = defaultdict(list)
    for ticker in selected_tickers:
        latest_complete = latest.get(ticker)
        if latest_complete is None:
            raise SystemExit(
                f"No complete raw_percent date found for {ticker}. "
                "Pass --start-date explicitly for first-time backfills."
            )
        start = next_business_day(latest_complete)
        if business_dates(start, end_date):
            groups[start].append(ticker)
        print(
            f"{ticker}: latest_complete={format_date(latest_complete)} "
            f"next_fetch={format_date(start)}",
            flush=True,
        )
    return dict(groups)


def ensure_aws_target(ssh_target, ssh_key, remote_data_dir, dry_run=False):
    run_command(
        [
            "ssh",
            "-i",
            ssh_key,
            ssh_target,
            f"sudo mkdir -p '{remote_data_dir}' && sudo chown -R $USER:$USER '{remote_data_dir}'",
        ],
        dry_run=dry_run,
    )


def rsync_to_aws(data_root, ssh_target, ssh_key, remote_data_dir, delete_remote=True, dry_run=False):
    cmd = [
        "rsync",
        "-avh",
        "--progress",
    ]
    if delete_remote:
        cmd.append("--delete")
    cmd.extend(
        [
            "-e",
            f"ssh -i {ssh_key}",
            "--exclude",
            ".DS_Store",
            f"{data_root.rstrip('/')}/",
            f"{ssh_target}:{remote_data_dir.rstrip('/')}/",
        ]
    )
    run_command(cmd, dry_run=dry_run)


def main():
    args = parse_args()

    selected_tickers = [item.strip() for item in args.tickers.split(",") if item.strip()]
    end_date = parse_date(args.end_date)
    data_root = Path(args.data_root)
    raw_dir = data_root / "raw_percent"

    repo_dir = Path(__file__).resolve().parent
    env = os.environ.copy()
    env["OPTIONS_DATA_ROOT"] = str(data_root)

    print("Daily options update", flush=True)
    print(f"Tickers: {','.join(selected_tickers)}", flush=True)
    print(f"Data root: {data_root}", flush=True)
    print(f"End date: {format_date(end_date)}", flush=True)

    fetched_tickers = set()
    fetch_start_dates = []

    if not args.skip_fetch:
        if not env.get("ICE_API_USERNAME") or not env.get("ICE_API_PASSWORD"):
            raise SystemExit("ICE_API_USERNAME and ICE_API_PASSWORD must be set in the environment.")

        fetch_groups = plan_fetch_groups(raw_dir, selected_tickers, args.start_date, end_date)

        if not fetch_groups:
            print("\nNo missing business dates to fetch.", flush=True)
        for start_date, group_tickers in sorted(fetch_groups.items()):
            ticker_csv = ",".join(group_tickers)
            fetched_tickers.update(group_tickers)
            fetch_start_dates.append(start_date)
            run_command(
                [
                    sys.executable,
                    str(repo_dir / "fetch_option_data.py"),
                    "--tickers",
                    ticker_csv,
                    "--start-date",
                    format_date(start_date),
                    "--end-date",
                    format_date(end_date),
                ],
                env=env,
                dry_run=args.dry_run,
            )

    downstream_tickers = sorted(fetched_tickers) if fetched_tickers else selected_tickers
    downstream_start = min(fetch_start_dates) if fetch_start_dates else (
        parse_date(args.start_date) if args.start_date else end_date
    )
    ticker_csv = ",".join(downstream_tickers)

    if not args.skip_master:
        run_command(
            [
                sys.executable,
                str(repo_dir / "sync_option_master.py"),
                "--tickers",
                ticker_csv,
                "--start-date",
                format_date(downstream_start),
                "--end-date",
                format_date(end_date),
            ],
            env=env,
            dry_run=args.dry_run,
        )

    if not args.skip_percentile:
        run_command(
            [
                sys.executable,
                str(repo_dir / "sync_option_percentile_master.py"),
                "--tickers",
                ticker_csv,
            ],
            env=env,
            dry_run=args.dry_run,
        )

    if not args.skip_strategies:
        run_command(
            [
                sys.executable,
                str(repo_dir / "sync_option_strategies_master.py"),
                "--tickers",
                ticker_csv,
            ],
            env=env,
            dry_run=args.dry_run,
        )

    if not args.skip_aws_sync:
        if not shutil.which("rsync"):
            raise SystemExit("rsync is required for AWS sync but was not found.")
        ensure_aws_target(args.ssh_target, args.ssh_key, args.remote_data_dir, dry_run=args.dry_run)
        rsync_to_aws(
            str(data_root),
            args.ssh_target,
            args.ssh_key,
            args.remote_data_dir,
            delete_remote=not args.no_delete_remote,
            dry_run=args.dry_run,
        )

    print("\nDaily options update complete.", flush=True)


if __name__ == "__main__":
    main()
