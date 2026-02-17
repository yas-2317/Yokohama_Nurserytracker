#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import csv
import json
import os
from pathlib import Path
from typing import Any, Dict, List, Set, Tuple

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
MASTER_CSV = DATA_DIR / "master_facilities.csv"
MONTHS_JSON = DATA_DIR / "months.json"

# 空欄なら全域 seed（推奨）
WARD_FILTER = (os.getenv("WARD_FILTER", "") or "").strip() or None
# どの月のJSONから seed するか（空欄なら最新月）
SEED_MONTH = (os.getenv("SEED_MONTH", "") or "").strip() or None


def safe(x: Any) -> str:
    return "" if x is None else str(x)


def load_months() -> List[str]:
    if not MONTHS_JSON.exists():
        raise RuntimeError("data/months.json が見つかりません")
    obj = json.loads(MONTHS_JSON.read_text(encoding="utf-8"))
    ms = obj.get("months") or []
    ms = [safe(m).strip() for m in ms if safe(m).strip()]
    if not ms:
        raise RuntimeError("months.json の months が空です")
    return sorted(ms)


def pick_seed_month(months: List[str]) -> str:
    if SEED_MONTH and SEED_MONTH in months:
        return SEED_MONTH
    return months[-1]  # 最新月


def read_master() -> Tuple[List[Dict[str, str]], List[str], Set[str]]:
    if not MASTER_CSV.exists():
        # master が無いなら新規作成
        fields = [
            "facility_id","name","ward","address","lat","lng","map_url",
            "facility_type","phone","website","notes",
            "nearest_station","walk_minutes",
            "name_kana","station_kana",
        ]
        return [], fields, set()

    with MASTER_CSV.open("r", encoding="utf-8-sig", newline="") as f:
        r = csv.DictReader(f)
        rows = list(r)
        fields = r.fieldnames or []
    ids = {safe(row.get("facility_id")).strip() for row in rows if safe(row.get("facility_id")).strip()}
    return rows, fields, ids


def write_master(rows: List[Dict[str, str]], fields: List[str]) -> None:
    # 必須カラムを末尾に足す（欠けてても壊れない）
    want = [
        "facility_id","name","ward","address","lat","lng","map_url",
        "facility_type","phone","website","notes",
        "nearest_station","walk_minutes",
        "name_kana","station_kana",
    ]
    for c in want:
        if c not in fields:
            fields.append(c)

    MASTER_CSV.parent.mkdir(parents=True, exist_ok=True)
    with MASTER_CSV.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for row in rows:
            w.writerow({k: row.get(k, "") for k in fields})


def in_scope_ward(ward: str) -> bool:
    if not WARD_FILTER:
        return True
    return WARD_FILTER in (ward or "")


def main() -> None:
    months = load_months()
    seed_month = pick_seed_month(months)

    p = DATA_DIR / f"{seed_month}.json"
    if not p.exists():
        raise RuntimeError(f"seed 元の月次JSONがありません: {p}")

    obj = json.loads(p.read_text(encoding="utf-8"))
    facs = obj.get("facilities") or []
    if not isinstance(facs, list):
        raise RuntimeError("月次JSONの facilities が list ではありません")

    rows, fields, existing_ids = read_master()

    added = 0
    scanned = 0

    for f in facs:
        if not isinstance(f, dict):
            continue
        fid = safe(f.get("id")).strip()
        name = safe(f.get("name")).strip()
        ward = safe(f.get("ward")).strip()

        if not fid:
            continue
        if not in_scope_ward(ward):
            continue

        scanned += 1
        if fid in existing_ids:
            continue

        # seed: まずは最低限（後で fix_master で埋める）
        rows.append({
            "facility_id": fid,
            "name": name,
            "ward": ward,
            "address": "",
            "lat": "",
            "lng": "",
            "map_url": "",
            "facility_type": "",
            "phone": "",
            "website": "",
            "notes": "",
            "nearest_station": "",
            "walk_minutes": "",
            "name_kana": "",
            "station_kana": "",
        })
        existing_ids.add(fid)
        added += 1

    write_master(rows, fields)

    print("DONE seed_master_from_month_json.py")
    print("  seed_month:", seed_month)
    print("  ward_filter:", WARD_FILTER if WARD_FILTER else "(none/all)")
    print("  facilities_in_month_json_scanned:", scanned)
    print("  added_rows_to_master:", added)
    print("  master_total_rows_now:", len(rows))


if __name__ == "__main__":
    main()
