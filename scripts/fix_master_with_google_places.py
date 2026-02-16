#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import csv
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
STATION_MISSES_CSV = DATA_DIR / "station_misses.csv"

# ---- env ----
API_KEY = (os.getenv("GOOGLE_MAPS_API_KEY") or "").strip()

WARD_FILTER = (os.getenv("WARD_FILTER") or "").strip() or None
MAX_UPDATES = int(os.getenv("MAX_UPDATES", "80"))

ONLY_BAD_ROWS = (os.getenv("ONLY_BAD_ROWS", "1") == "1")
STRICT_ADDRESS_CHECK = (os.getenv("STRICT_ADDRESS_CHECK", "1") == "1")

SLEEP_SEC = float(os.getenv("GOOGLE_API_SLEEP_SEC", "0.15"))

OVERWRITE_PHONE = (os.getenv("OVERWRITE_PHONE", "0") == "1")
OVERWRITE_WEBSITE = (os.getenv("OVERWRITE_WEBSITE", "0") == "1")
OVERWRITE_MAP_URL = (os.getenv("OVERWRITE_MAP_URL", "0") == "1")

# station/walk
FILL_NEAREST_STATION = (os.getenv("FILL_NEAREST_STATION", "1") == "1")
OVERWRITE_NEAREST_STATION = (os.getenv("OVERWRITE_NEAREST_STATION", "1") == "1")
OVERWRITE_WALK_MINUTES = (os.getenv("OVERWRITE_WALK_MINUTES", "1") == "1")


def norm(s: Any) -> str:
    if s is None:
        return ""
    x = str(s).replace("　", " ")
    x = re.sub(r"\s+", " ", x).strip()
    return x


def to_float(s: Any) -> Optional[float]:
    if s is None:
        return None
    t = str(s).strip()
    if t == "":
        return None
    try:
        return float(t)
    except Exception:
        return None


def to_int(s: Any) -> Optional[int]:
    if s is None:
        return None
    t = str(s).strip()
    if t == "":
        return None
    try:
        return int(float(t))
    except Exception:
        return None


def ok_address(addr: str, ward: Optional[str]) -> bool:
    if not STRICT_ADDRESS_CHECK:
        return True
    if not addr:
        return False
    # 横浜市が入ってる & （WARD_FILTERが指定されてるなら区も入る）をざっくりチェック
    if "横浜市" not in addr:
        return False
    if ward and ward not in addr:
        return False
    return True


def maps_get(url: str, params: Dict[str, str], timeout: int = 30) -> Dict[str, Any]:
    if not API_KEY:
        raise RuntimeError("GOOGLE_MAPS_API_KEY が未設定です（GitHub Secretsに設定してください）")
    params = dict(params)
    params["key"] = API_KEY
    r = requests.get(url, params=params, timeout=timeout)
    r.raise_for_status()
    data = r.json()
    status = data.get("status")
    if status not in ("OK", "ZERO_RESULTS"):
        raise RuntimeError(f"Google API error: status={status} error_message={data.get('error_message')}")
    time.sleep(SLEEP_SEC)
    return data


def places_text_search(query: str) -> Optional[Dict[str, Any]]:
    url = "https://maps.googleapis.com/maps/api/place/textsearch/json"
    data = maps_get(url, {"query": query, "language": "ja", "region": "jp"})
    results = data.get("results") or []
    return results[0] if results else None


def place_details(place_id: str) -> Optional[Dict[str, Any]]:
    url = "https://maps.googleapis.com/maps/api/place/details/json"
    fields = ",".join(
        [
            "name",
            "formatted_address",
            "geometry/location",
            "url",
            "website",
            "formatted_phone_number",
            "types",
        ]
    )
    data = maps_get(url, {"place_id": place_id, "fields": fields, "language": "ja"})
    return (data.get("result") or None)


def nearby_station(lat: float, lng: float) -> Optional[Tuple[str, str]]:
    """
    近傍の駅名とplace_idを返す（railway_station / train_station）
    """
    url = "https://maps.googleapis.com/maps/api/place/nearbysearch/json"
    loc = f"{lat},{lng}"
    # 半径は好みで。徒歩圏じゃなくても最寄り駅としてはOKなので1500m
    data = maps_get(
        url,
        {
            "location": loc,
            "radius": "1500",
            "type": "train_station",
            "language": "ja",
        },
    )
    results = data.get("results") or []
    if not results:
        # railway_station を優先したい場合の保険（typeが違うケース）
        data = maps_get(
            url,
            {
                "location": loc,
                "radius": "1500",
                "type": "railway_station",
                "language": "ja",
            },
        )
        results = data.get("results") or []
        if not results:
            return None
    top = results[0]
    return (top.get("name") or "").strip(), (top.get("place_id") or "").strip()


def walking_minutes(origin_lat: float, origin_lng: float, dest_place_id: str) -> Optional[int]:
    """
    Distance Matrixで徒歩分数を取る
    """
    if not dest_place_id:
        return None
    url = "https://maps.googleapis.com/maps/api/distancematrix/json"
    origins = f"{origin_lat},{origin_lng}"
    destinations = f"place_id:{dest_place_id}"
    data = maps_get(
        url,
        {
            "origins": origins,
            "destinations": destinations,
            "mode": "walking",
            "language": "ja",
            "region": "jp",
        },
        timeout=60,
    )
    rows = data.get("rows") or []
    if not rows:
        return None
    elems = rows[0].get("elements") or []
    if not elems:
        return None
    el = elems[0]
    if el.get("status") != "OK":
        return None
    dur = el.get("duration", {}).get("value")  # seconds
    if dur is None:
        return None
    return int(round(float(dur) / 60.0))


def build_query(name: str, ward: str, address: str) -> str:
    parts = [name]
    if address:
        parts.append(address)
    if ward:
        parts.append(f"横浜市{ward}")
    else:
        parts.append("横浜市")
    parts.append("保育園")
    return " ".join([p for p in parts if p]).strip()


def should_update_row(row: Dict[str, str]) -> bool:
    """
    ONLY_BAD_ROWS=1 の場合は、埋めたい主要項目が欠けている行だけ更新対象にする
    """
    if not ONLY_BAD_ROWS:
        return True

    addr = (row.get("address") or "").strip()
    lat = (row.get("lat") or "").strip()
    lng = (row.get("lng") or "").strip()
    st = (row.get("nearest_station") or "").strip()
    wm = (row.get("walk_minutes") or "").strip()

    # 住所が空 or lat/lngが無い or 駅/徒歩が無い → 更新対象
    if addr == "":
        return True
    if lat == "" or lng == "":
        return True
    if FILL_NEAREST_STATION and (st == "" or wm == ""):
        return True
    return False


def ensure_headers(rows: List[Dict[str, str]]) -> List[str]:
    """
    master_facilities.csv のヘッダが足りない場合もあるので、必要列を保証する
    """
    base = [
        "facility_id",
        "name",
        "ward",
        "address",
        "lat",
        "lng",
        "facility_type",
        "phone",
        "website",
        "notes",
        "nearest_station",
        "walk_minutes",
        "map_url",
    ]
    existing = list(rows[0].keys()) if rows else base
    for k in base:
        if k not in existing:
            existing.append(k)
    return existing


def read_master() -> List[Dict[str, str]]:
    if not MASTER_CSV.exists():
        raise RuntimeError("data/master_facilities.csv が見つかりません")
    with MASTER_CSV.open("r", encoding="utf-8-sig", newline="") as f:
        rows = list(csv.DictReader(f))
    if not rows:
        raise RuntimeError("master_facilities.csv が空です")
    return rows


def write_master(rows: List[Dict[str, str]], fieldnames: List[str]) -> None:
    with MASTER_CSV.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow({k: (r.get(k, "") if r.get(k, "") is not None else "") for k in fieldnames})


def write_station_misses(misses: List[Dict[str, str]]) -> None:
    # 0件でも作る（デバッグが楽）
    with STATION_MISSES_CSV.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["facility_id", "name", "ward", "reason"])
        w.writeheader()
        for r in misses:
            w.writerow(r)


def main() -> None:
    print("START fix_master_with_google_places.py")
    print("WARD_FILTER=", WARD_FILTER, "MAX_UPDATES=", MAX_UPDATES, "ONLY_BAD_ROWS=", ONLY_BAD_ROWS)

    rows = read_master()
    fieldnames = ensure_headers(rows)

    target = WARD_FILTER.strip() if WARD_FILTER else None

    updated_cells = 0
    updated_rows = 0

    station_misses: List[Dict[str, str]] = []

    for r in rows:
        if updated_rows >= MAX_UPDATES:
            break

        fid = (r.get("facility_id") or "").strip()
        name = (r.get("name") or "").strip()
        ward = (r.get("ward") or "").strip()

        if not fid or not name:
            continue
        if target and target not in ward:
            continue
        if not should_update_row(r):
            continue

        # 現状値
        addr0 = (r.get("address") or "").strip()
        lat0 = to_float(r.get("lat"))
        lng0 = to_float(r.get("lng"))
        map0 = (r.get("map_url") or "").strip()

        # 1) Places検索 → details
        query = build_query(name, ward, addr0)
        top = places_text_search(query)
        if not top:
            # 施設が拾えない → station missとして記録（lat/lngない場合は駅算出できない）
            station_misses.append({"facility_id": fid, "name": name, "ward": ward, "reason": "place_not_found"})
            continue

        place_id = (top.get("place_id") or "").strip()
        det = place_details(place_id) if place_id else None
        if not det:
            station_misses.append({"facility_id": fid, "name": name, "ward": ward, "reason": "place_details_failed"})
            continue

        # detailsから取得
        addr = (det.get("formatted_address") or "").strip()
        loc = (det.get("geometry") or {}).get("location") or {}
        lat = loc.get("lat")
        lng = loc.get("lng")

        phone = (det.get("formatted_phone_number") or "").strip()
        website = (det.get("website") or "").strip()
        gmap_url = (det.get("url") or "").strip()
        types = det.get("types") or []
        facility_type = (r.get("facility_type") or "").strip()
        if not facility_type and types:
            facility_type = ",".join(types)

        # addressの妥当性
        if addr and not ok_address(addr, ward):
            # 住所が怪しい → 住所は上書きしないが、lat/lngは使える場合がある
            addr = ""

        # 2) 値の反映（上書き条件）
        def set_if(field: str, new_val: str, overwrite: bool) -> int:
            nonlocal r
            cur = (r.get(field) or "").strip()
            if new_val is None:
                return 0
            new_val = str(new_val).strip()
            if new_val == "":
                return 0
            if (cur == "") or overwrite:
                if cur != new_val:
                    r[field] = new_val
                    return 1
            return 0

        # address/lat/lng/map_url は基本空欄補完（住所は妥当な時だけ）
        if addr:
            updated_cells += set_if("address", addr, overwrite=False)

        if lat is not None and lng is not None:
            # lat/lng は空欄なら補完
            if (r.get("lat") or "").strip() == "":
                r["lat"] = f"{lat:.7f}"
                updated_cells += 1
            if (r.get("lng") or "").strip() == "":
                r["lng"] = f"{lng:.7f}"
                updated_cells += 1

        # map_url
        if gmap_url:
            updated_cells += set_if("map_url", gmap_url, overwrite=OVERWRITE_MAP_URL)
        elif map0 == "" and lat is not None and lng is not None:
            # gmap_urlが取れない場合の保険：座標検索URL
            coord_url = f"https://www.google.com/maps/search/?api=1&query={lat},{lng}"
            updated_cells += set_if("map_url", coord_url, overwrite=OVERWRITE_MAP_URL)

        # phone / website
        if phone:
            updated_cells += set_if("phone", phone, overwrite=OVERWRITE_PHONE)
        if website:
            updated_cells += set_if("website", website, overwrite=OVERWRITE_WEBSITE)

        if facility_type:
            updated_cells += set_if("facility_type", facility_type, overwrite=False)

        # 3) station/walk
        lat_use = to_float(r.get("lat")) or lat0
        lng_use = to_float(r.get("lng")) or lng0

        if FILL_NEAREST_STATION:
            st_cur = (r.get("nearest_station") or "").strip()
            wm_cur = (r.get("walk_minutes") or "").strip()

            need_station = (st_cur == "") or OVERWRITE_NEAREST_STATION
            need_walk = (wm_cur == "") or OVERWRITE_WALK_MINUTES

            if (need_station or need_walk):
                if lat_use is None or lng_use is None:
                    station_misses.append({"facility_id": fid, "name": name, "ward": ward, "reason": "no_latlng"})
                else:
                    st = nearby_station(lat_use, lng_use)
                    if not st:
                        station_misses.append({"facility_id": fid, "name": name, "ward": ward, "reason": "station_not_found"})
                    else:
                        st_name, st_pid = st
                        if st_name:
                            updated_cells += set_if("nearest_station", st_name, overwrite=OVERWRITE_NEAREST_STATION)
                        if st_pid:
                            wm = walking_minutes(lat_use, lng_use, st_pid)
                            if wm is None:
                                station_misses.append({"facility_id": fid, "name": name, "ward": ward, "reason": "walk_time_failed"})
                            else:
                                updated_cells += set_if("walk_minutes", str(wm), overwrite=OVERWRITE_WALK_MINUTES)

        updated_rows += 1

    write_master(rows, fieldnames)
    write_station_misses(station_misses)

    print("DONE. wrote:", str(MASTER_CSV))
    print("updated rows:", updated_rows)
    print("updated cells:", updated_cells)
    print("station misses:", len(station_misses), "->", str(STATION_MISSES_CSV))


if __name__ == "__main__":
    main()
