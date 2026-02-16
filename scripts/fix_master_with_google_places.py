#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import csv
import os
import re
import time
import math
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests

# ========= Config =========
ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

MASTER_CSV = DATA_DIR / "master_facilities.csv"
UPDATES_CSV = DATA_DIR / "master_facilities_updates.csv"
GEOCODE_MISSES_CSV = DATA_DIR / "geocode_misses.csv"
STATION_MISSES_CSV = DATA_DIR / "station_misses.csv"

API_KEY = (os.getenv("GOOGLE_MAPS_API_KEY", "") or "").strip()
if not API_KEY:
    raise RuntimeError("GOOGLE_MAPS_API_KEY が空です（GitHub Secrets に設定してください）")

WARD_FILTER = (os.getenv("WARD_FILTER", "港北区") or "").strip() or None

MAX_UPDATES = int(os.getenv("MAX_UPDATES", "80"))  # 1回で更新する最大件数
ONLY_BAD_ROWS = (os.getenv("ONLY_BAD_ROWS", "1") == "1")  # 怪しい行だけ更新
STRICT_ADDRESS_CHECK = (os.getenv("STRICT_ADDRESS_CHECK", "1") == "1")  # 横浜市+区が入る住所だけ採用
SLEEP_SEC = float(os.getenv("GOOGLE_API_SLEEP_SEC", "0.15"))

OVERWRITE_PHONE = (os.getenv("OVERWRITE_PHONE", "0") == "1")
OVERWRITE_WEBSITE = (os.getenv("OVERWRITE_WEBSITE", "0") == "1")
OVERWRITE_MAP_URL = (os.getenv("OVERWRITE_MAP_URL", "0") == "1")

# ★追加：最寄り駅/徒歩
FILL_NEAREST_STATION = (os.getenv("FILL_NEAREST_STATION", "1") == "1")
OVERWRITE_NEAREST_STATION = (os.getenv("OVERWRITE_NEAREST_STATION", "0") == "1")
OVERWRITE_WALK_MINUTES = (os.getenv("OVERWRITE_WALK_MINUTES", "0") == "1")

# ========= Utils =========
def norm(s: Any) -> str:
    if s is None:
        return ""
    x = str(s).replace("　", " ")
    x = re.sub(r"\s+", " ", x).strip()
    return x

def is_blank(s: Any) -> bool:
    return (s is None) or (str(s).strip() == "")

def should_update_cell(current: str, overwrite: bool) -> bool:
    return overwrite or is_blank(current)

def try_float(s: Any) -> Optional[float]:
    if s is None:
        return None
    t = str(s).strip()
    if t == "":
        return None
    try:
        return float(t)
    except Exception:
        return None

def ceil_minutes(seconds: Optional[int]) -> Optional[int]:
    if seconds is None:
        return None
    return int(math.ceil(seconds / 60.0))

def http_get(url: str, params: Dict[str, Any], timeout: int = 30) -> Dict[str, Any]:
    r = requests.get(url, params=params, timeout=timeout)
    r.raise_for_status()
    return r.json()

def build_query(name: str, ward: str, address: str) -> str:
    # 住所が変なケースでも name + ward で引けるように
    parts = [name, address, ward, "横浜市", "日本"]
    parts = [p for p in parts if p and str(p).strip()]
    q = " ".join(parts)
    q = re.sub(r"\s+", " ", q).strip()
    return q

def ok_address(addr: str) -> bool:
    if not STRICT_ADDRESS_CHECK:
        return True
    if "横浜市" not in addr:
        return False
    if WARD_FILTER and WARD_FILTER not in addr:
        return False
    return True

# ========= Google APIs (Geocode + Places Details-ish via Place search) =========
def geocode_text_search(query: str) -> Optional[Dict[str, Any]]:
    """
    住所・座標を得る：Geocoding API
    """
    url = "https://maps.googleapis.com/maps/api/geocode/json"
    data = http_get(url, {"address": query, "key": API_KEY, "language": "ja", "region": "jp"}, timeout=30)
    if data.get("status") != "OK":
        return None
    results = data.get("results", [])
    if not results:
        return None
    return results[0]

def places_find_by_text(query: str) -> Optional[Dict[str, Any]]:
    """
    place_id / name / formatted_address / geometry / website / phone を拾う
    （Places API Legacy: Find Place From Text）
    """
    url = "https://maps.googleapis.com/maps/api/place/findplacefromtext/json"
    params = {
        "input": query,
        "inputtype": "textquery",
        "fields": "place_id,name,formatted_address,geometry",
        "language": "ja",
        "region": "jp",
        "key": API_KEY,
    }
    data = http_get(url, params, timeout=30)
    if data.get("status") != "OK":
        return None
    cands = data.get("candidates", [])
    if not cands:
        return None
    return cands[0]

def places_details(place_id: str) -> Optional[Dict[str, Any]]:
    """
    phone/website を補完（Places Details Legacy）
    """
    url = "https://maps.googleapis.com/maps/api/place/details/json"
    params = {
        "place_id": place_id,
        "fields": "formatted_phone_number,website,url",
        "language": "ja",
        "region": "jp",
        "key": API_KEY,
    }
    data = http_get(url, params, timeout=30)
    if data.get("status") != "OK":
        return None
    return data.get("result") or None

# ========= Nearest station + walking time =========
def places_nearest_station(lat: float, lng: float) -> Optional[Dict[str, Any]]:
    """
    近傍の駅を探す：Places Nearby Search (Legacy)
    rankby=distance を使う（最寄り順）
    """
    url = "https://maps.googleapis.com/maps/api/place/nearbysearch/json"

    # まず train_station → ダメなら subway_station → transit_station
    types = ["train_station", "subway_station", "transit_station"]

    for t in types:
        params = {
            "location": f"{lat},{lng}",
            "rankby": "distance",
            "type": t,
            "language": "ja",
            "region": "jp",
            "key": API_KEY,
        }
        data = http_get(url, params, timeout=30)
        status = data.get("status")
        if status not in ("OK", "ZERO_RESULTS"):
            # OVER_QUERY_LIMIT 等は上に投げるより、ここは None にして後でmissにする
            return None
        results = data.get("results", [])
        if not results:
            continue

        # “駅” っぽい名前を優先（保険）
        for r in results[:10]:
            name = str(r.get("name", "")).strip()
            if "駅" in name:
                return r
        return results[0]

    return None

def distance_matrix_walk_minutes(lat: float, lng: float, dest_place_id: str) -> Optional[int]:
    """
    徒歩分：Distance Matrix API（destinations に place_id:xxx を指定） :contentReference[oaicite:2]{index=2}
    """
    url = "https://maps.googleapis.com/maps/api/distancematrix/json"
    params = {
        "origins": f"{lat},{lng}",
        "destinations": f"place_id:{dest_place_id}",
        "mode": "walking",
        "language": "ja",
        "region": "jp",
        "key": API_KEY,
    }
    data = http_get(url, params, timeout=30)
    if data.get("status") != "OK":
        return None
    rows = data.get("rows", [])
    if not rows:
        return None
    els = rows[0].get("elements", [])
    if not els:
        return None
    el = els[0]
    if el.get("status") != "OK":
        return None
    dur = el.get("duration", {})
    sec = dur.get("value")
    if sec is None:
        return None
    return ceil_minutes(int(sec))

# ========= Row judgement =========
def looks_bad_row(row: Dict[str, str]) -> bool:
    """
    ざっくり「住所が空/緯度経度が無い/住所が変」のどれかなら“修正対象”
    """
    addr = (row.get("address") or "").strip()
    lat = (row.get("lat") or "").strip()
    lng = (row.get("lng") or "").strip()
    # 住所が空 or 横浜市が入ってない（雑）
    if addr == "" or ("横浜市" not in addr):
        return True
    # 座標が無い
    if lat == "" or lng == "":
        return True
    return False

# ========= Main =========
def main() -> None:
    if not MASTER_CSV.exists():
        raise RuntimeError(f"{MASTER_CSV} が見つかりません")

    with MASTER_CSV.open("r", encoding="utf-8-sig", newline="") as f:
        rows = list(csv.DictReader(f))

    # ensure columns exist
    cols = rows[0].keys() if rows else []
    need_cols = [
        "facility_id","name","ward","address","lat","lng","facility_type","phone","website","notes",
        "nearest_station","walk_minutes","map_url"
    ]
    for nc in need_cols:
        if nc not in cols:
            # 無い列は追加して後で書き出す
            pass

    updated = 0
    geocode_misses: List[Dict[str, str]] = []
    station_misses: List[Dict[str, str]] = []
    updates_log: List[Dict[str, str]] = []

    for r in rows:
        ward = (r.get("ward") or "").strip()
        if WARD_FILTER and WARD_FILTER not in ward:
            continue

        if ONLY_BAD_ROWS and (not looks_bad_row(r)):
            # bad rows のみ対象
            continue

        if updated >= MAX_UPDATES:
            break

        fid = (r.get("facility_id") or "").strip()
        name = (r.get("name") or "").strip()
        addr0 = (r.get("address") or "").strip()

        query = build_query(name, ward, addr0)
        # まず FindPlace で place_id/geometry を得る（精度と後続が楽）
        place = places_find_by_text(query)
        time.sleep(SLEEP_SEC)

        if not place:
            # Geocoding も試す
            g = geocode_text_search(query)
            time.sleep(SLEEP_SEC)
            if not g:
                geocode_misses.append({"facility_id": fid, "name": name, "ward": ward, "query_tried": query})
                continue

            loc = (((g.get("geometry") or {}).get("location")) or {})
            lat = loc.get("lat")
            lng = loc.get("lng")
            fmt_addr = (g.get("formatted_address") or "").replace("日本、", "")
            if fmt_addr and not ok_address(fmt_addr):
                geocode_misses.append({"facility_id": fid, "name": name, "ward": ward, "query_tried": query})
                continue

            # apply geocode result
            changed = False
            if fmt_addr and should_update_cell(r.get("address",""), overwrite=True):
                r["address"] = fmt_addr
                changed = True
            if lat is not None and should_update_cell(r.get("lat",""), overwrite=True):
                r["lat"] = str(lat)
                changed = True
            if lng is not None and should_update_cell(r.get("lng",""), overwrite=True):
                r["lng"] = str(lng)
                changed = True

            if changed:
                updates_log.append({"facility_id": fid, "field": "geocode", "value": "applied"})
                updated += 1

        else:
            # FindPlace から geometry/address
            pid = place.get("place_id")
            loc = ((place.get("geometry") or {}).get("location")) or {}
            lat = loc.get("lat")
            lng = loc.get("lng")
            fmt_addr = (place.get("formatted_address") or "").replace("日本、", "")

            if fmt_addr and not ok_address(fmt_addr):
                geocode_misses.append({"facility_id": fid, "name": name, "ward": ward, "query_tried": query})
                continue

            changed_any = False

            if fmt_addr and should_update_cell(r.get("address",""), overwrite=True):
                r["address"] = fmt_addr
                changed_any = True
            if lat is not None and should_update_cell(r.get("lat",""), overwrite=True):
                r["lat"] = str(lat)
                changed_any = True
            if lng is not None and should_update_cell(r.get("lng",""), overwrite=True):
                r["lng"] = str(lng)
                changed_any = True

            # details (phone/website/map_url)
            details = places_details(pid) if pid else None
            time.sleep(SLEEP_SEC)

            if details:
                phone = (details.get("formatted_phone_number") or "").strip()
                web = (details.get("website") or "").strip()
                gurl = (details.get("url") or "").strip()  # Google Maps URL

                if phone and should_update_cell(r.get("phone",""), OVERWRITE_PHONE):
                    r["phone"] = phone
                    changed_any = True
                    updates_log.append({"facility_id": fid, "field": "phone", "value": phone})

                if web and should_update_cell(r.get("website",""), OVERWRITE_WEBSITE):
                    r["website"] = web
                    changed_any = True
                    updates_log.append({"facility_id": fid, "field": "website", "value": web})

                if gurl and should_update_cell(r.get("map_url",""), OVERWRITE_MAP_URL):
                    r["map_url"] = gurl
                    changed_any = True
                    updates_log.append({"facility_id": fid, "field": "map_url", "value": gurl})

            # ★最寄り駅 + 徒歩分
            if FILL_NEAREST_STATION:
                latf = try_float(r.get("lat"))
                lngf = try_float(r.get("lng"))
                if latf is not None and lngf is not None:
                    need_station = should_update_cell(r.get("nearest_station",""), OVERWRITE_NEAREST_STATION)
                    need_walk = should_update_cell(r.get("walk_minutes",""), OVERWRITE_WALK_MINUTES)

                    if need_station or need_walk:
                        st = places_nearest_station(latf, lngf)
                        time.sleep(SLEEP_SEC)

                        if not st:
                            station_misses.append({"facility_id": fid, "name": name, "ward": ward, "reason": "no_station_found"})
                        else:
                            st_name = str(st.get("name","")).strip()
                            st_pid = str(st.get("place_id","")).strip()

                            if st_name and need_station:
                                r["nearest_station"] = st_name
                                changed_any = True
                                updates_log.append({"facility_id": fid, "field": "nearest_station", "value": st_name})

                            if st_pid and need_walk:
                                mins = distance_matrix_walk_minutes(latf, lngf, st_pid)
                                time.sleep(SLEEP_SEC)
                                if mins is None:
                                    station_misses.append({"facility_id": fid, "name": name, "ward": ward, "reason": "distance_matrix_failed"})
                                else:
                                    r["walk_minutes"] = str(mins)
                                    changed_any = True
                                    updates_log.append({"facility_id": fid, "field": "walk_minutes", "value": str(mins)})

            if changed_any:
                updated += 1

    # Write back master csv (keep header stable)
    # collect all columns from existing + required
    all_cols = set()
    for r in rows:
        all_cols |= set(r.keys())
    # ensure required columns exist
    for c in [
        "facility_id","name","ward","address","lat","lng","facility_type","phone","website","notes",
        "nearest_station","walk_minutes","map_url"
    ]:
        all_cols.add(c)

    # keep a nice order
    ordered = [
        "facility_id","name","ward","address","lat","lng",
        "facility_type","phone","website","map_url",
        "nearest_station","walk_minutes","notes",
    ]
    rest = [c for c in sorted(all_cols) if c not in ordered]
    fieldnames = ordered + rest

    with MASTER_CSV.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            # ensure all keys exist
            out = {k: r.get(k, "") for k in fieldnames}
            w.writerow(out)

    # logs
    if updates_log:
        with UPDATES_CSV.open("w", encoding="utf-8-sig", newline="") as f:
            w = csv.DictWriter(f, fieldnames=["facility_id","field","value"])
            w.writeheader()
            for x in updates_log:
                w.writerow(x)

    if geocode_misses:
        with GEOCODE_MISSES_CSV.open("w", encoding="utf-8-sig", newline="") as f:
            w = csv.DictWriter(f, fieldnames=["facility_id","name","ward","query_tried"])
            w.writeheader()
            for x in geocode_misses:
                w.writerow(x)

    if station_misses:
        with STATION_MISSES_CSV.open("w", encoding="utf-8-sig", newline="") as f:
            w = csv.DictWriter(f, fieldnames=["facility_id","name","ward","reason"])
            w.writeheader()
            for x in station_misses:
                w.writerow(x)

    print("DONE.")
    print("total rows:", len(rows))
    print("updated rows:", updated)
    print("geocode misses:", len(geocode_misses))
    print("station misses:", len(station_misses))
    print("wrote:", str(MASTER_CSV))


if __name__ == "__main__":
    main()
