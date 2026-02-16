#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import csv
import json
import os
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
MASTER_CSV = DATA_DIR / "master_facilities.csv"

# 0: 既存値が空のときだけ埋める / 1: master があれば上書き
OVERWRITE = (os.getenv("APPLY_MASTER_OVERWRITE", "0") == "1")

def safe(s: Any) -> str:
    return "" if s is None else str(s)

def read_master() -> Dict[str, Dict[str, str]]:
    if not MASTER_CSV.exists():
        raise RuntimeError("data/master_facilities.csv がありません")
    out: Dict[str, Dict[str, str]] = {}
    with MASTER_CSV.open("r", encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            fid = safe(row.get("facility_id")).strip()
            if fid:
                out[fid] = row
    return out

def should_set(current: Any, newv: Any) -> bool:
    if newv is None:
        return False
    nv = safe(newv).strip()
    if nv == "":
        return False
    if OVERWRITE:
        return True
    cv = safe(current).strip()
    return (cv == "")

def apply_to_facility(f: Dict[str, Any], m: Dict[str, str]) -> int:
    """
    f: month json facility
    m: master row
    return: changed field count
    """
    changed = 0

    # 追加/上書きしたいフィールド（HPで使う＆検索/ソート用）
    fields = [
        "address", "lat", "lng", "map_url",
        "facility_type", "phone", "website", "notes",
        "nearest_station", "walk_minutes",
        "name_kana", "station_kana",
    ]

    for k in fields:
        if k not in m:
            continue
        if should_set(f.get(k), m.get(k)):
            f[k] = m.get(k)
            changed += 1

    # walk_minutes は数値として入っていた方が JS が楽なので整形
    if "walk_minutes" in f and f["walk_minutes"] not in (None, ""):
        try:
            f["walk_minutes"] = int(float(f["walk_minutes"]))
        except Exception:
            # 変換できない場合はそのまま
            pass

    return changed

def list_month_jsons() -> List[Path]:
    # data/2026-02-01.json のような形式だけ対象
    out: List[Path] = []
    for p in DATA_DIR.glob("*.json"):
        if p.name == "months.json":
            continue
        if re.match(r"^\d{4}-\d{2}-\d{2}\.json$", p.name):
            out.append(p)
    return sorted(out)

def main() -> None:
    master = read_master()
    files = list_month_jsons()
    if not files:
        print("No month json files found under data/")
        return

    total_fac = 0
    total_changed = 0
    touched_files = 0

    for p in files:
        obj = json.loads(p.read_text(encoding="utf-8"))
        facs = obj.get("facilities") or []
        changed_in_file = 0

        for f in facs:
            fid = safe(f.get("id")).strip()
            if not fid:
                continue
            m = master.get(fid)
            if not m:
                continue
            total_fac += 1
            changed_in_file += apply_to_facility(f, m)

        if changed_in_file > 0:
            p.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")
            touched_files += 1
            total_changed += changed_in_file
            print(f"UPDATED {p.name}: +{changed_in_file} fields")

    print("DONE")
    print("month files:", len(files), "touched:", touched_files)
    print("facilities visited:", total_fac)
    print("total fields updated:", total_changed)

if __name__ == "__main__":
    main()
