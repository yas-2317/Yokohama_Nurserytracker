#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import csv
import json
import math
import os
import re
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

MASTER_CSV = DATA_DIR / "master_facilities.csv"
OUT_CSV = DATA_DIR / "master_facilities.csv"  # 上書き
CACHE_JSON = DATA_DIR / "geocode_cache.json"

WARD_HINT = (os.getenv("WARD_FILTER", "港北区") or "").strip() or "港北区"

# 徒歩分の換算（m/分）
WALK_SPEED_M_PER_MIN = float(os.getenv("WALK_SPEED_M_PER_MIN", "80"))
# Nominatim レート制限対策（秒）
SLEEP_SEC = float(os.getenv("NOMINATIM_SLEEP_SEC", "1.1"))
# 失敗を減らすための試行回数
RETRY = int(os.getenv("NOMINATIM_RETRY", "3"))

# ---- 港北区周辺 主要駅（必要なら追加） ----
STATIONS: List[Dict[str, Any]] = [
    {"name": "日吉駅", "lat": 35.5533, "lng": 139.6467},
    {"name": "綱島駅", "lat": 35.5366, "lng": 139.6340},
    {"name": "大倉山駅", "lat": 35.5228, "lng": 139.6296},
    {"name": "菊名駅", "lat": 35.5096, "lng": 139.6305},
    {"name": "新横浜駅", "lat": 35.5069, "lng": 139.6170},
    {"name": "妙蓮寺駅", "lat": 35.4978, "lng": 139.6346},
    {"name": "白楽駅", "lat": 35.4868, "lng": 139.6250},
    {"name": "小机駅", "lat": 35.5153, "lng": 139.5978},
    {"name": "新羽駅", "lat": 35.5270, "lng": 139.6119},
    {"name": "北新横浜駅", "lat": 35.5186, "lng": 139.6091},
    {"name": "高田駅", "lat": 35.5484, "lng": 139.6146},
    {"name": "日吉本町駅", "lat": 35.5557, "lng": 139.6318},
    {"name": "岸根公園駅", "lat": 35.4937, "lng": 139.6123},
]

# ---------------- utils ----------------
def norm(s: Any) -> str:
    if s is None:
        return ""
    x = str(s).replace("　", " ").strip()
    x = re.sub(r"\s+", " ", x)
    return x.strip()

def is_blank(s: Any) -> bool:
    return norm(s) == ""

def haversine_m(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    R = 6371000.0
    p1 = math.radians(lat1)
    p2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dl = math.radians(lng2 - lng1)
    a = math.sin(dphi/2)**2 + math.cos(p1) * math.cos(p2) * math.sin(dl/2)**2
    return 2 * R * math.asin(math.sqrt(a))

def load_cache() -> Dict[str, Any]:
    if CACHE_JSON.exists():
        try:
            return json.loads(CACHE_JSON.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}

def save_cache(cache: Dict[str, Any]) -> None:
    CACHE_JSON.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")

def guess_nearest_station(lat: float, lng: float) -> Tuple[str, int]:
    best_name = ""
    best_m = 10**18
    for st in STATIONS:
        d = haversine_m(lat, lng, float(st["lat"]), float(st["lng"]))
        if d < best_m:
            best_m = d
            best_name = st["name"]
    walk_min = int(math.ceil(best_m / WALK_SPEED_M_PER_MIN))
    return best_name, walk_min

def build_map_url(lat: float, lng: float) -> str:
    return f"https://www.google.com/maps/search/?api=1&query={lat},{lng}"

# ---------------- nominatim ----------------
def nominatim_search(q: str) -> Optional[Dict[str, Any]]:
    url = "https://nominatim.openstreetmap.org/search"
    headers = {
        # ここは必ず入れる（User-Agent必須）
        "User-Agent": "NurseryAvailabilityBot/1.0 (non-commercial; github actions)",
        "Accept-Language": "ja",
    }
    params = {
        "q": q,
        "format": "json",
        "limit": 1,
        "addressdetails": 1,
    }

    last_err = None
    for t in range(RETRY):
        try:
            r = requests.get(url, params=params, headers=headers, timeout=40)
            # Nominatimは429が出ることがあるので少し待ってリトライ
            if r.status_code == 429:
                time.sleep(max(SLEEP_SEC, 2.0) * (t + 1))
                continue
            r.raise_for_status()
            arr = r.json()
            if not arr:
                return None
            return arr[0]
        except Exception as e:
            last_err = e
            time.sleep(SLEEP_SEC * (t + 1))
    if last_err:
        print("WARN nominatim failed:", last_err)
    return None

def lookup_nominatim(name: str, ward: str, address_hint: str, cache: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    # キャッシュキーは “ward::name”
    key = f"{ward}::{name}"
    if key in cache:
        return cache[key]

    # クエリは段階的に試す（取りこぼしを減らす）
    # 住所が空なら園名＋横浜市＋区、住所があるなら住所も足す
    queries = []
    if address_hint:
        queries.append(f"{name} {address_hint} 横浜市{ward} 日本")
    queries.append(f"{name} 横浜市{ward} 日本")
    # “保育園” の別表記がある園もいるので保険
    if "保育園" not in name:
        queries.append(f"{name} 保育園 横浜市{ward} 日本")

    hit = None
    used_q = ""
    for q in queries:
        q = re.sub(r"\s+", " ", q).strip()
        used_q = q
        hit = nominatim_search(q)
        # 失敗してもレートのため少し待つ
        time.sleep(SLEEP_SEC)
        if hit:
            break

    if not hit:
        return None

    lat = float(hit["lat"])
    lng = float(hit["lon"])
    disp = hit.get("display_name") or ""

    out = {
        "address": disp,  # 長い場合があるが、まずはそのまま。必要なら後で整形。
        "lat": lat,
        "lng": lng,
        "map_url": build_map_url(lat, lng),
        "q": used_q,
    }

    cache[key] = out
    save_cache(cache)
    return out

# ---------------- csv i/o ----------------
def read_csv(path: Path) -> List[Dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))

def write_csv(path: Path, rows: List[Dict[str, str]], fieldnames: List[str]) -> None:
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in fieldnames})

def ensure_columns(fieldnames: List[str]) -> List[str]:
    needed = [
        "facility_id","name","ward","address","lat","lng",
        "facility_type","phone","website","notes",
        "nearest_station","walk_minutes","map_url"
    ]
    for k in needed:
        if k not in fieldnames:
            fieldnames.append(k)
    return fieldnames

def main() -> None:
    if not MASTER_CSV.exists():
        raise FileNotFoundError(f"not found: {MASTER_CSV}")

    rows = read_csv(MASTER_CSV)
    if not rows:
        raise RuntimeError("master_facilities.csv is empty")

    fieldnames = ensure_columns(list(rows[0].keys()))
    cache = load_cache()

    updated = 0
    geocoded = 0
    for i, r in enumerate(rows, 1):
        name = norm(r.get("name"))
        if not name:
            continue

        ward = norm(r.get("ward")) or WARD_HINT
        address_hint = norm(r.get("address"))

        # address/lat/lng/map_url のうち、どれか欠けてたらジオコード
        need_geo = (
            is_blank(r.get("address")) or
            is_blank(r.get("lat")) or is_blank(r.get("lng")) or
            is_blank(r.get("map_url"))
        )

        if need_geo:
            out = lookup_nominatim(name, ward, address_hint, cache)
            if out:
                if is_blank(r.get("address")) and out.get("address"):
                    r["address"] = str(out["address"])
                if (is_blank(r.get("lat")) or is_blank(r.get("lng"))) and out.get("lat") is not None and out.get("lng") is not None:
                    r["lat"] = str(out["lat"])
                    r["lng"] = str(out["lng"])
                if is_blank(r.get("map_url")) and out.get("map_url"):
                    r["map_url"] = str(out["map_url"])
                geocoded += 1
                updated += 1

        # 最寄り駅・徒歩分（lat/lngが揃ったら推定）
        try:
            lat = float(r.get("lat") or 0)
            lng = float(r.get("lng") or 0)
            if lat != 0 and lng != 0:
                if is_blank(r.get("nearest_station")) or is_blank(r.get("walk_minutes")):
                    st, wm = guess_nearest_station(lat, lng)
                    if is_blank(r.get("nearest_station")):
                        r["nearest_station"] = st
                        updated += 1
                    if is_blank(r.get("walk_minutes")):
                        r["walk_minutes"] = str(wm)
                        updated += 1
        except Exception:
            pass

        if i % 50 == 0:
            print(f"processed {i}/{len(rows)} ... geocoded={geocoded} updated={updated}")

    write_csv(OUT_CSV, rows, fieldnames)
    print("DONE. wrote:", OUT_CSV)
    print("geocoded rows:", geocoded)
    print("updated cells:", updated)

if __name__ == "__main__":
    main()
