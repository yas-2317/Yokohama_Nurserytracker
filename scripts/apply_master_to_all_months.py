#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import csv
import json
import re
from pathlib import Path
from typing import Any, Dict

from pykakasi import kakasi

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
MASTER_CSV = DATA_DIR / "master_facilities.csv"

_kks = kakasi()
_kks.setMode("J", "H")
_kks.setMode("K", "H")
_kks.setMode("H", "H")
_conv = _kks.getConverter()

def hira(s: Any) -> str:
    s = "" if s is None else str(s)
    s = s.strip()
    if not s:
        return ""
    s = _conv.do(s)
    s = s.replace("　", " ")
    s = re.sub(r"\s+", "", s)
    return s

def station_base(s: str) -> str:
    s = (s or "").strip()
    return s[:-1].strip() if s.endswith("駅") else s

def load_master() -> Dict[str, Dict[str, str]]:
    out: Dict[str, Dict[str, str]] = {}
    if not MASTER_CSV.exists():
        return out
    with MASTER_CSV.open("r", encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            fid = (row.get("facility_id") or "").strip()
            if fid:
                out[fid] = row
    return out

def main() -> None:
    master = load_master()
    if not master:
        raise RuntimeError("master_facilities.csv が見つからない/空です")

    changed_files = 0
    changed_rows = 0

    for p in sorted(DATA_DIR.glob("*.json")):
        if p.name == "months.json":
            continue

        obj = json.loads(p.read_text(encoding="utf-8"))
        facs = obj.get("facilities") or []
        dirty = False

        for f in facs:
            fid = str(f.get("id") or "").strip()
            if not fid:
                continue

            m = master.get(fid)
            if not m:
                continue

            # masterの値を反映（空欄は無理に上書きしない）
            for k in ["address","lat","lng","map_url","facility_type","phone","website","notes","nearest_station","walk_minutes","name_kana","station_kana"]:
                mv = (m.get(k) or "").strip()
                if mv != "":
                    f[k] = mv

            # kanaが空なら生成して埋める（上書き可）
            if not (f.get("name_kana") or "").strip():
                f["name_kana"] = hira(f.get("name") or "")
            if not (f.get("station_kana") or "").strip():
                base = station_base(f.get("nearest_station") or "")
                f["station_kana"] = hira(base)

            dirty = True

        if dirty:
            p.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")
            changed_files += 1
            changed_rows += len(facs)

    print(f"DONE. updated files={changed_files}, touched facility rows={changed_rows}")

if __name__ == "__main__":
    main()
