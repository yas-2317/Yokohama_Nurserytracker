#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import csv
import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
MASTER_CSV = DATA_DIR / "master_facilities.csv"
MONTHS_JSON = DATA_DIR / "months.json"

# 空欄なら全域に適用（推奨）
WARD_FILTER = (os.getenv("WARD_FILTER", "") or "").strip() or None


def safe(x: Any) -> str:
    return "" if x is None else str(x)


def as_int_str(x: Any) -> Optional[str]:
    s = safe(x).strip()
    if s == "" or s.lower() == "null" or s == "-":
        return None
    try:
        return str(int(float(s)))
    except Exception:
        return None


def load_master() -> Dict[str, Dict[str, str]]:
    if not MASTER_CSV.exists():
        raise RuntimeError("data/master_facilities.csv が見つかりません")
    out: Dict[str, Dict[str, str]] = {}
    with MASTER_CSV.open("r", encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            fid = safe(row.get("facility_id")).strip()
            if fid:
                out[fid] = {k: safe(v) for k, v in row.items()}
    return out


def load_months_from_months_json() -> List[str]:
    if not MONTHS_JSON.exists():
        return []
    try:
        obj = json.loads(MONTHS_JSON.read_text(encoding="utf-8"))
        ms = obj.get("months") or []
        return [safe(m).strip() for m in ms if safe(m).strip()]
    except Exception:
        return []


def scan_months_from_files() -> List[str]:
    # data/ の YYYY-MM-01.json を拾う（months.json が欠けてても回す）
    ms: List[str] = []
    for p in DATA_DIR.glob("*.json"):
        name = p.name
        if name == "months.json":
            continue
        # 2026-02-01.json の形だけ拾う
        if len(name) == len("2026-02-01.json") and name[4] == "-" and name[7] == "-" and name.endswith(".json"):
            ms.append(name.replace(".json", ""))
    return sorted(set(ms))


def in_scope_ward(ward: str) -> bool:
    if not WARD_FILTER:
        return True
    return WARD_FILTER in (ward or "")


def apply_master_to_facility(f: Dict[str, Any], m: Dict[str, str]) -> int:
    updated = 0

    mapping = {
        "address": "address",
        "lat": "lat",
        "lng": "lng",
        "map_url": "map_url",
        "facility_type": "facility_type",
        "phone": "phone",
        "website": "website",
        "notes": "notes",
        "nearest_station": "nearest_station",
        "name_kana": "name_kana",
        "station_kana": "station_kana",
    }

    for jkey, mkey in mapping.items():
        mv = safe(m.get(mkey)).strip()
        if mv == "":
            continue
        cur = safe(f.get(jkey)).strip()
        if cur != mv:
            f[jkey] = mv
            updated += 1

    wm = as_int_str(m.get("walk_minutes"))
    if wm is not None:
        cur = safe(f.get("walk_minutes")).strip()
        if cur != wm:
            f["walk_minutes"] = wm
            updated += 1

    return updated


def main() -> None:
    master = load_master()

    # months.json + フォールバック（ファイル走査）
    months_a = load_months_from_months_json()
    months_b = scan_months_from_files()
    months = sorted(set(months_a) | set(months_b))

    if not months:
        raise RuntimeError("対象月が見つかりません（data/months.json か data/*.json を確認）")

    total_files = 0
    total_facilities = 0
    total_updates = 0
    changed_files: List[str] = []

    print("APPLY master → month JSONs")
    print("  months(from months.json):", len(months_a))
    print("  months(from file scan):", len(months_b))
    print("  months(total unique):", len(months))
    print("  ward_filter:", WARD_FILTER if WARD_FILTER else "(none/all)")

    for month in months:
        p = DATA_DIR / f"{month}.json"
        if not p.exists():
            continue

        obj = json.loads(p.read_text(encoding="utf-8"))
        facs = obj.get("facilities") or []
        if not isinstance(facs, list):
            continue

        changed = False
        file_updates = 0
        file_fac_count = 0

        for f in facs:
            if not isinstance(f, dict):
                continue

            fid = safe(f.get("id")).strip()
            ward = safe(f.get("ward")).strip()

            if not fid:
                continue
            if not in_scope_ward(ward):
                continue

            m = master.get(fid)
            if not m:
                continue

            u = apply_master_to_facility(f, m)
            if u > 0:
                changed = True
                file_updates += u
            file_fac_count += 1

        if changed:
            p.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")
            changed_files.append(month)

        total_files += 1
        total_facilities += file_fac_count
        total_updates += file_updates

        print(f"[{month}] scanned={file_fac_count} updates={file_updates} changed={changed}")

    print("DONE apply_master_to_all_months.py")
    print("  files_seen:", total_files)
    print("  facilities_scanned:", total_facilities)
    print("  updated_cells:", total_updates)
    print("  changed_months:", len(changed_files))
    if changed_files:
        print("  changed_months_list:", ", ".join(changed_files[:30]) + (" ..." if len(changed_files) > 30 else ""))


if __name__ == "__main__":
    main()
