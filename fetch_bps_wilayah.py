#!/usr/bin/env python3
"""Fetch Indonesian regional data from the BPS bridging API and emit SQL dumps.

The script performs a breadth-first traversal across the supplied administrative
levels, captures raw JSON responses, normalizes them into tabular records, and
finally writes SQL statements that mirror the formatting of `data/wilayah.sql`.
It can also query the BPS periode catalogue to list available snapshots or
automatically pick the latest periode value.

Typical usage:
    python scripts/fetch_bps_wilayah.py \
        --periode-merge latest \
        --levels provinsi,kabupaten,kecamatan \
        --cookie "BIGipServer=...; TS017=..."

The resulting artifacts are stored under `data/raw/bps/<periode>/`,
`data/processed/bps/<periode>/`, and `data/sql/bps_wilayah_<periode>.sql`.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import sys
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, Iterable, List, Optional

try:
    import requests
except ImportError as exc:  # pragma: no cover - defensive guard
    sys.stderr.write("error: requests library is required (pip install requests)\n")
    raise

BASE_HEADERS = {
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
    "DNT": "1",
    "Referer": "https://sig.bps.go.id/bridging-kode/index",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36",
    "X-Requested-With": "XMLHttpRequest",
    "sec-ch-ua": '"Not=A?Brand";v="24", "Chromium";v="140"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
}

LEVEL_ORDER = ["provinsi", "kabupaten", "kecamatan", "desa"]
PERIODE_ENDPOINT = "https://sig.bps.go.id/rest-drop-down/getperiode"
SQL_TABLE_NAME = "bps_wilayah"
SQL_COLUMNS = [
    "kode_bps",
    "nama_bps",
    "kode_dagri",
    "nama_dagri",
    "level",
    "parent_kode_bps",
    "periode_merge",
    "fetched_at",
]


class FetchError(RuntimeError):
    """Raised when the API repeatedly fails."""


def log(message: str, verbose: bool) -> None:
    if verbose:
        sys.stderr.write(f"{message}\n")


def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--periode-merge",
        default="latest",
        help="BPS periode_merge parameter; pass 'latest' to auto-discover",
    )
    parser.add_argument(
        "--levels",
        default="provinsi,kabupaten,kecamatan",
        help="Comma-separated list of levels to traverse in order",
    )
    parser.add_argument(
        "--base-url",
        default="https://sig.bps.go.id/rest-bridging/getwilayah",
        help="Base URL for the BPS bridging API",
    )
    parser.add_argument(
        "--periode-url",
        default=PERIODE_ENDPOINT,
        help="Endpoint for retrieving available periode values",
    )
    parser.add_argument(
        "--cookie",
        default="",
        help="Cookie header to include (e.g. BIGipServer=...; TS017=...)",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.25,
        help="Seconds to sleep between requests to avoid hammering the API",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=30.0,
        help="Request timeout in seconds",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=3,
        help="Maximum retries per request on non-200 responses",
    )
    parser.add_argument(
        "--raw-dir",
        type=Path,
        default=Path("data/raw/bps"),
        help="Root directory for raw JSON payloads",
    )
    parser.add_argument(
        "--processed-dir",
        type=Path,
        default=Path("data/processed/bps"),
        help="Root directory for normalized CSV outputs",
    )
    parser.add_argument(
        "--sql-dir",
        type=Path,
        default=Path("data/sql"),
        help="Directory for generated SQL dumps",
    )
    parser.add_argument(
        "--sql-filename",
        default="",
        help="Override the SQL filename (defaults to bps_wilayah_<periode>.sql)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Skip HTTP requests and only show which URLs would be fetched",
    )
    parser.add_argument(
        "--list-periodes",
        action="store_true",
        help="List available periodes and exit",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Emit verbose progress logs to stderr",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=8,
        help="Maximum concurrent requests per level",
    )
    return parser.parse_args(argv)


def normalize_levels(levels_str: str) -> List[str]:
    levels = [lvl.strip().lower() for lvl in levels_str.split(",") if lvl.strip()]
    unknown = [lvl for lvl in levels if lvl not in LEVEL_ORDER]
    if unknown:
        raise ValueError(f"Unsupported levels supplied: {', '.join(unknown)}")
    # Preserve caller order but respect LEVEL_ORDER precedence
    levels.sort(key=lambda lvl: LEVEL_ORDER.index(lvl))
    return levels


def build_headers(cookie: str) -> Dict[str, str]:
    headers = dict(BASE_HEADERS)
    if cookie:
        headers["Cookie"] = cookie
    return headers


def fetch_level(
    base_url: str,
    level: str,
    parent: str,
    periode_merge: str,
    headers: Dict[str, str],
    timeout: float,
    retries: int,
    delay: float,
    dry_run: bool,
    verbose: bool,
) -> List[Dict[str, str]]:
    params = {
        "level": level,
        "parent": parent,
        "periode_merge": periode_merge,
    }
    if dry_run:
        sys.stdout.write(f"DRY-RUN: would request {base_url} with {params}\n")
        return []

    for attempt in range(1, retries + 1):
        log(
            f"Requesting level={level} parent={parent or '-'} attempt={attempt}",
            verbose,
        )
        response = requests.get(base_url, params=params, headers=headers, timeout=timeout)
        if response.status_code == 200:
            try:
                payload = response.json()
            except ValueError as exc:  # pragma: no cover - network failure
                raise FetchError(f"Invalid JSON for level={level} parent={parent}: {exc}")
            size = len(payload) if isinstance(payload, list) else "?"
            log(
                f"Received {size} records for level={level} parent={parent or '-'}",
                verbose,
            )
            return payload
        log(
            f"HTTP {response.status_code} for level={level} parent={parent or '-'}",
            verbose,
        )
        time.sleep(delay * attempt)
    raise FetchError(
        f"Failed to fetch level={level} parent={parent} after {retries} attempts"
    )


def fetch_periodes(
    session: requests.Session,
    periode_url: str,
    headers: Dict[str, str],
    timeout: float,
    retries: int,
    delay: float,
    dry_run: bool,
    verbose: bool,
) -> object:
    if dry_run:
        sys.stdout.write(f"DRY-RUN: would request {periode_url} for periodes\n")
        return []

    for attempt in range(1, retries + 1):
        log(f"Requesting periode catalogue attempt={attempt}", verbose)
        response = session.get(periode_url, headers=headers, timeout=timeout)
        if response.status_code == 200:
            try:
                payload = response.json()
            except ValueError as exc:  # pragma: no cover
                raise FetchError(f"Invalid JSON from periode endpoint: {exc}")
            size = len(payload) if isinstance(payload, list) else "?"
            log(f"Received periode catalogue with {size} entries", verbose)
            return payload
        log(f"HTTP {response.status_code} while requesting periode catalogue", verbose)
        time.sleep(delay * attempt)
    raise FetchError(f"Failed to fetch periodes after {retries} attempts")


def extract_periode_values(raw_payload: object) -> List[str]:
    values: List[str] = []

    def derive(item: object) -> Optional[str]:
        if isinstance(item, str):
            return item.strip() or None
        if isinstance(item, dict):
            for key in (
                "periode",
                "periode_merge",
                "value",
                "kode",
                "kode_periode",
            ):
                value = item.get(key)
                if isinstance(value, str) and value.strip():
                    return value.strip()
        return None

    if isinstance(raw_payload, dict):
        # Some responses wrap data under "data" or similar
        for key in ("data", "rows", "items", "result"):
            child = raw_payload.get(key)
            if isinstance(child, list):
                raw_payload = child  # type: ignore[assignment]
                break
        else:
            raw_payload = list(raw_payload.values())  # type: ignore[assignment]

    if isinstance(raw_payload, list):
        for item in raw_payload:
            period = derive(item)
            if period:
                values.append(period)

    # Deduplicate while preserving order
    seen = set()
    ordered: List[str] = []
    for value in values:
        if value not in seen:
            ordered.append(value)
            seen.add(value)
    return ordered


def crawl_hierarchy(
    base_url: str,
    levels: List[str],
    periode_merge: str,
    headers: Dict[str, str],
    timeout: float,
    retries: int,
    delay: float,
    dry_run: bool,
    verbose: bool,
    workers: int,
) -> Dict[str, List[Dict[str, Optional[str]]]]:
    """Fetch all requested levels and attach parent relationships."""
    collected: Dict[str, List[Dict[str, Optional[str]]]] = {level: [] for level in levels}
    province_lookup: Dict[str, str] = {}
    seen_codes: Dict[str, set[str]] = {level: set() for level in levels}

    parents: List[str] = [""]
    for idx, level in enumerate(levels):
        next_parents: List[str] = []
        results: Dict[str, List[Dict[str, str]]] = {}

        if dry_run or len(parents) == 1:
            # Fall back to sequential to keep dry-run output readable or when only root.
            for parent in parents:
                results[parent] = fetch_level(
                    base_url,
                    level,
                    parent,
                    periode_merge,
                    headers,
                    timeout,
                    retries,
                    delay,
                    dry_run,
                    verbose,
                )
        else:
            max_workers = max(1, workers)
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_map = {
                    executor.submit(
                        fetch_level,
                        base_url,
                        level,
                        parent,
                        periode_merge,
                        headers,
                        timeout,
                        retries,
                        delay,
                        dry_run,
                        verbose,
                    ): parent
                    for parent in parents
                }
                for future in as_completed(future_map):
                    parent = future_map[future]
                    try:
                        payload = future.result()
                    except Exception as exc:  # propagate with context
                        raise FetchError(
                            f"Failed to fetch level={level} parent={parent}: {exc}"
                        ) from exc
                    results[parent] = payload

        for parent in parents:
            payload = results.get(parent, [])
            for item in payload:
                kode_bps = str(item.get("kode_bps", "")).strip()
                if not kode_bps or kode_bps == "0":
                    log(
                        f"Skipping level={level} parent={parent or '-'} with empty kode_bps",
                        verbose,
                    )
                    continue
                if kode_bps in seen_codes[level]:
                    log(
                        f"Skipping duplicate level={level} kode_bps={kode_bps}",
                        verbose,
                    )
                    continue
                record = {
                    "level": level,
                    "kode_bps": kode_bps,
                    "nama_bps": str(item.get("nama_bps", "")).strip(),
                    "kode_dagri": str(item.get("kode_dagri", "")).strip(),
                    "nama_dagri": str(item.get("nama_dagri", "")).strip(),
                    "parent_kode_bps": parent or None,
                }
                seen_codes[level].add(kode_bps)
                if idx == 0:
                    province_lookup[kode_bps] = kode_bps
                    record["province_kode_bps"] = kode_bps
                else:
                    record["province_kode_bps"] = province_lookup.get(parent, parent)
                    province_lookup[kode_bps] = record["province_kode_bps"]
                collected[level].append(record)
                if idx + 1 < len(levels):
                    next_parents.append(kode_bps)

        parents = next_parents
        time.sleep(delay)
    return collected


def ensure_output_dirs(raw_dir: Path, processed_dir: Path, sql_dir: Path, periode: str) -> Dict[str, Path]:
    raw_root = raw_dir / periode
    processed_root = processed_dir / periode
    sql_dir.mkdir(parents=True, exist_ok=True)
    raw_root.mkdir(parents=True, exist_ok=True)
    processed_root.mkdir(parents=True, exist_ok=True)
    return {"raw": raw_root, "processed": processed_root}


def persist_raw(raw_root: Path, level: str, parents_payload: List[Dict[str, object]]) -> None:
    raw_file = raw_root / f"{level}.json"
    raw_file.write_text(
        json.dumps(
            {
                "level": level,
                "fetched_at": dt.datetime.now(dt.timezone.utc).isoformat(),
                "payloads": parents_payload,
            },
            ensure_ascii=False,
            indent=2,
        )
    )


def persist_processed(processed_root: Path, level: str, rows: List[Dict[str, Optional[str]]], periode: str, fetched_at: str) -> None:
    processed_file = processed_root / f"{level}.csv"
    fieldnames = [
        "level",
        "kode_bps",
        "nama_bps",
        "kode_dagri",
        "nama_dagri",
        "parent_kode_bps",
        "periode_merge",
        "fetched_at",
        "province_kode_bps",
    ]
    with processed_file.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            payload = dict(row)
            payload.update({
                "periode_merge": periode,
                "fetched_at": fetched_at,
            })
            writer.writerow(payload)


def write_manifest(processed_root: Path, periode: str, levels: List[str], counts: Dict[str, int], fetched_at: str, base_url: str) -> None:
    manifest = {
        "periode_merge": periode,
        "fetched_at": fetched_at,
        "levels": levels,
        "counts": counts,
        "base_url": base_url,
    }
    (processed_root / "manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8"
    )


def sql_escape(value: Optional[str]) -> str:
    if value is None or value == "":
        return "NULL"
    escaped = str(value).replace("'", "''")
    return f"'{escaped}'"


def render_sql(
    all_records: List[Dict[str, Optional[str]]],
    periode: str,
    levels: List[str],
    fetched_at: str,
) -> str:
    header_lines = [
        "/*",
        "BPS wilayah dump generated by scripts/fetch_bps_wilayah.py",
        "Source : https://sig.bps.go.id/",
        f"Periode: {periode}",
        f"Fetched: {fetched_at}",
        f"Levels : {', '.join(levels)}",
        "*/",
        "--",
        "-- Table structure for table bps_wilayah",
        "--",
        "",
        "DROP TABLE IF EXISTS bps_wilayah;",
        "CREATE TABLE IF NOT EXISTS bps_wilayah (",
        "    kode_bps VARCHAR(16) NOT NULL,",
        "    nama_bps VARCHAR(200) NOT NULL,",
        "    kode_dagri VARCHAR(16),",
        "    nama_dagri VARCHAR(200),",
        "    level VARCHAR(16) NOT NULL,",
        "    parent_kode_bps VARCHAR(16),",
        "    periode_merge VARCHAR(32) NOT NULL,",
        "    fetched_at VARCHAR(32) NOT NULL,",
        "    PRIMARY KEY (level, kode_bps)",
        ") ENGINE=MyISAM;",
        "CREATE INDEX bps_wilayah_parent_idx ON bps_wilayah (parent_kode_bps);",
        "",
        "--",
        "-- Dumping data for table bps_wilayah",
        "--",
        "",
    ]

    # Group records by province for readability
    by_province: Dict[str, List[Dict[str, Optional[str]]]] = defaultdict(list)
    province_names: Dict[str, str] = {}
    for record in all_records:
        province_code = record.get("province_kode_bps") or record["kode_bps"]
        by_province[province_code].append(record)
        if record["level"] == "provinsi":
            province_names[province_code] = record.get("nama_bps", province_code)
    level_rank = {lvl: idx for idx, lvl in enumerate(LEVEL_ORDER)}

    province_blocks: List[str] = []
    for province_code in sorted(by_province.keys()):
        province_name = province_names.get(province_code, province_code)
        province_records = by_province[province_code]
        province_records.sort(key=lambda rec: (level_rank.get(rec["level"], 99), rec["kode_bps"]))
        lines = [f"-- Provinsi {province_name}", "INSERT INTO bps_wilayah (" + ", ".join(SQL_COLUMNS) + ")", "VALUES"]
        tuples: List[str] = []
        for idx, record in enumerate(province_records):
            values = [
                sql_escape(record.get("kode_bps")),
                sql_escape(record.get("nama_bps")),
                sql_escape(record.get("kode_dagri")),
                sql_escape(record.get("nama_dagri")),
                sql_escape(record.get("level")),
                sql_escape(record.get("parent_kode_bps")),
                sql_escape(periode),
                sql_escape(fetched_at),
            ]
            tuple_str = f"({', '.join(values)})"
            if idx + 1 < len(province_records):
                tuple_str += ","
            else:
                tuple_str += ";"
            tuples.append(tuple_str)
        lines.extend(tuples)
        province_blocks.append("\n".join(lines))
        province_blocks.append("")

    return "\n".join(header_lines + province_blocks)


def flatten_records(collected: Dict[str, List[Dict[str, Optional[str]]]]) -> List[Dict[str, Optional[str]]]:
    merged: List[Dict[str, Optional[str]]] = []
    for level in LEVEL_ORDER:
        rows = collected.get(level, [])
        if rows:
            merged.extend(rows)
    return merged


def select_periode(
    raw_value: str,
    session: requests.Session,
    periode_url: str,
    headers: Dict[str, str],
    timeout: float,
    retries: int,
    delay: float,
    dry_run: bool,
    verbose: bool,
) -> str:
    value = (raw_value or "").strip()
    if not value or value.lower() == "latest":
        if dry_run:
            sys.stdout.write(
                "DRY-RUN: skipping periode discovery; using placeholder 'latest'\n"
            )
            return "latest"
        payload = fetch_periodes(
            session=session,
            periode_url=periode_url,
            headers=headers,
            timeout=timeout,
            retries=retries,
            delay=delay,
            dry_run=dry_run,
            verbose=verbose,
        )
        periods = extract_periode_values(payload)
        if not periods:
            raise FetchError("No periode values returned; cannot continue")
        log(f"Auto-selected latest periode: {periods[0]}", verbose)
        return periods[0]
    return value


def main(argv: Optional[Iterable[str]] = None) -> None:
    args = parse_args(argv)
    levels = normalize_levels(args.levels)
    headers = build_headers(args.cookie)
    session = requests.Session()
    verbose = args.verbose
    log(f"Levels to fetch: {', '.join(levels)}", verbose)

    if args.list_periodes:
        payload = fetch_periodes(
            session=session,
            periode_url=args.periode_url,
            headers=headers,
            timeout=args.timeout,
            retries=args.max_retries,
            delay=args.delay,
            dry_run=args.dry_run,
            verbose=verbose,
        )
        values = extract_periode_values(payload)
        if not values:
            sys.stdout.write("No periode values found.\n")
        else:
            sys.stdout.write("Available periodes:\n")
            for value in values:
                sys.stdout.write(f"  - {value}\n")
        return

    periode = select_periode(
        raw_value=args.periode_merge,
        session=session,
        periode_url=args.periode_url,
        headers=headers,
        timeout=args.timeout,
        retries=args.max_retries,
        delay=args.delay,
        dry_run=args.dry_run,
        verbose=verbose,
    )
    log(f"Using periode: {periode}", verbose)
    fetched_at = dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat()

    sql_filename = (
        args.sql_filename
        if args.sql_filename
        else f"bps_wilayah_{periode.replace('/', '-')}.sql"
    )
    sql_path = args.sql_dir / sql_filename

    if args.dry_run:
        output_paths = {
            "raw": args.raw_dir / periode,
            "processed": args.processed_dir / periode,
        }
    else:
        output_paths = ensure_output_dirs(
            args.raw_dir, args.processed_dir, args.sql_dir, periode
        )
    collected = crawl_hierarchy(
        base_url=args.base_url,
        levels=levels,
        periode_merge=periode,
        headers=headers,
        timeout=args.timeout,
        retries=args.max_retries,
        delay=args.delay,
        dry_run=args.dry_run,
        verbose=verbose,
        workers=args.workers,
    )

    counts = {level: len(collected[level]) for level in levels}
    for level, count in counts.items():
        log(f"Collected {count} rows for level={level}", verbose)

    for level in levels:
        # Persist raw payloads grouped by parent for traceability
        if not args.dry_run:
            parent_payload = []
            parents = sorted({row.get("parent_kode_bps") or "" for row in collected[level]})
            for parent in parents:
                parent_payload.append({
                    "parent_kode_bps": parent or None,
                    "items": [
                        row
                        for row in collected[level]
                        if (row.get("parent_kode_bps") or "") == (parent or "")
                    ],
                })
            persist_raw(output_paths["raw"], level, parent_payload)
            persist_processed(output_paths["processed"], level, collected[level], periode, fetched_at)

    if not args.dry_run:
        write_manifest(output_paths["processed"], periode, levels, counts, fetched_at, args.base_url)

    all_records = flatten_records(collected)
    if not args.dry_run:
        sql_content = render_sql(all_records, periode, levels, fetched_at)
        sql_path.write_text(sql_content, encoding="utf-8")

    summary_lines = [
        "BPS wilayah extraction completed.",
        f"Levels        : {', '.join(levels)}",
        f"Periode       : {periode}",
        f"Fetched at    : {fetched_at}",
        f"Raw payloads  : {output_paths['raw']}",
        f"Processed CSV : {output_paths['processed']}",
        f"SQL output    : {sql_path}",
    ]
    for level in levels:
        summary_lines.append(f"  - {level:<10}: {counts[level]} rows")
    summary_lines.append(f"Workers      : {args.workers}")
    sys.stdout.write("\n".join(summary_lines) + "\n")


if __name__ == "__main__":
    main()
