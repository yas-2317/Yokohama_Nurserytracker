#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import csv
import json
import os
from pathlib import Path
from typing import Any, Dict, List, Tuple

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

MASTER_CSV = DATA_DIR / "master_facilities.csv"
MONTHS_JSON = DATA_DIR / "months.json"

# 空欄なら全域。指定すればその区だけ master 増殖できる（例: 港北区）
WARD_FILTER = (os.getenv("WARD_FILTER", "") or "").strip() or None


def safe(x: Any) -> str:
    return "" if x is None else str(x)


def read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def load_months() -> List[str]:
    if not MONTHS_JSON.exists():
        raise RuntimeError("data/months.json が見つかりません（先に backfill / update を実行して生成してください）")
    obj = read_json(MONTHS_JSON)
    ms = obj.get("months") or []
    return [safe(m).strip() for m in ms if safe(m).strip()]


def ensure_master_schema(fieldnames: List[str]) -> List[str]:
    want = [
        "facility_id",
        "name",
        "ward",
        "address",
        "lat",
        "lng",
        "map_url",
        "facility_type",
        "phone",
        "website",
        "notes",
        "nearest_station",
        "walk_minutes",
        "name_kana",
        "station_kana",
    ]
    out = list(fieldnames) if fieldnames else []
    for c in want:
        if c not in out:
            out.append(c)
    return out


def load_master() -> Tuple[List[Dict[str, str]], List[str], Dict[str, Dict[str, str]]]:
    if not MASTER_CSV.exists():
        # masterが無ければ空から作る
        fields = ensure_master_schema([])
        return [], fields, {}

    with MASTER_CSV.open("r", encoding="utf-8-sig", newline="") as f:
        r = csv.DictReader(f)
        rows = list(r)
        fields = ensure_master_schema(r.fieldnames or [])

    mp: Dict[str, Dict[str, str]] = {}
    for row in rows:
        fid = safe(row.get("facility_id")).strip()
        if fid:
            mp[fid] = row
    return rows, fields, mp


def write_master(rows: List[Dict[str, str]], fields: List[str]) -> None:
    with MASTER_CSV.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for r in rows:
            w.writerow({k: safe(r.get(k)) for k in fields})


def in_ward_scope(ward: str) -> bool:
    if not WARD_FILTER:
        return True
    return WARD_FILTER in (ward or "")


def scan_facilities_from_month(month: str) -> List[Dict[str, str]]:
    p = DATA_DIR / f"{month}.json"
    if not p.exists():
        return []

    obj = read_json(p)
    facs = obj.get("facilities") or []
    if not isinstance(facs, list):
        return []

    out: List[Dict[str, str]] = []
    for f in facs:
        if not isinstance(f, dict):
            continue
        fid = safe(f.get("id")).strip()
        name = safe(f.get("name")).strip()
        ward = safe(f.get("ward")).strip()
        if not fid or not name:
            continue
        if not in_ward_scope(ward):
            continue
        out.append({"facility_id": fid, "name": name, "ward": ward})
    return out


def main() -> None:
    months = load_months()
    rows, fields, master_map = load_master()

    # 既存masterの行順維持しつつ、足りないIDを末尾に追加
    existing_ids = set(master_map.keys())

    added = 0
    updated_name_ward = 0
    scanned = 0

    # 月の古い順→新しい順で見て、後の月ほど“最新名称/区”として採用されやすい
    for m in sorted(months):
        facs = scan_facilities_from_month(m)
        for f in facs:
            scanned += 1
            fid = f["facility_id"]
            name = f["name"]
            ward = f["ward"]

            if fid in master_map:
                # name / ward が空なら補完（既存に値があれば壊さない）
                cur = master_map[fid]
                changed = False
                if safe(cur.get("name")).strip() == "" and name:
                    cur["name"] = name
                    changed = True
                if safe(cur.get("ward")).strip() == "" and ward:
                    cur["ward"] = ward
                    changed = True
                if changed:
                    updated_name_ward += 1
                continue

            # 新規行（他の列は空）
            new_row: Dict[str, str] = {k: "" for k in fields}
            new_row["facility_id"] = fid
            new_row["name"] = name
            new_row["ward"] = ward

            rows.append(new_row)
            master_map[fid] = new_row
            existing_ids.add(fid)
            added += 1

    # 施設IDでソートしたい場合はここで rows を並べ替える（任意）
    # rows.sort(key=lambda r: safe(r.get("facility_id")))

    write_master(rows, fields)

    print("DONE expand_master_from_months.py")
    print(f"  months={len(months)} scanned_facility_rows={scanned}")
    print(f"  master_total_rows={len(rows)} added_rows={added} updated_name_ward_rows={updated_name_ward}")
    if WARD_FILTER:
        print(f"  ward_filter={WARD_FILTER}")
    else:
        print("  ward_filter=(none / all wards)")


if __name__ == "__main__":
    main()
