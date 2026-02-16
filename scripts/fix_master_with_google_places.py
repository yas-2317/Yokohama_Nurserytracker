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

# ★全域対応：駅キャッシュを汎用化（市全域を基本にし、必要なら区別キャッシュも可能）
STATION_CACHE = DATA_DIR / "stations_cache_yokohama.json"
STATION_MISSES = DATA_DIR / "station_misses.csv"

GOOGLE_MAPS_API_KEY = (os.getenv("GOOGLE_MAPS_API_KEY") or "").strip()
GOOGLE_API_SLEEP_SEC = float(os.getenv("GOOGLE_API_SLEEP_SEC", "0.15"))

MAX_UPDATES = int(os.getenv("MAX_UPDATES", "200"))
ONLY_BAD_ROWS = os.getenv("ONLY_BAD_ROWS", "0") == "1"
STRICT_ADDRESS_CHECK = os.getenv("STRICT_ADDRESS_CHECK", "1") == "1"

# 対象エリア（将来は「横浜市全域」を基本にする）
CITY_FILTER = (os.getenv("CITY_FILTER", "横浜市") or "").strip() or "横浜市"
WARD_FILTER = (os.getenv("WARD_FILTER", "") or "").strip() or None  # 空なら全区

OVERWRITE_PHONE = os.getenv("OVERWRITE_PHONE", "0") == "1"
OVERWRITE_WEBSITE = os.getenv("OVERWRITE_WEBSITE", "0") == "1"
OVERWRITE_MAP_URL = os.getenv("OVERWRITE_MAP_URL", "0") == "1"

FILL_NEAREST_STATION = os.getenv("FILL_NEAREST_STATION", "1") == "1"
OVERWRITE_NEAREST_STATION = os.getenv("OVERWRITE_NEAREST_STATION", "1") == "1"
OVERWRITE_WALK_MINUTES = os.getenv("OVERWRITE_WALK_MINUTES", "1") == "1"

# 駅探索半径（m）
NEARBY_RADIUS_M = int(os.getenv("NEARBY_RADIUS_M", "2500"))

# ★駅キャッシュ作成：N件くらいまで自動収集（多すぎるとコストが増えるので上限を持つ）
STATION_SEED_LIMIT = int(os.getenv("STATION_SEED_LIMIT", "180"))  # Yokohama: 駅が多いので上限
FORCE_REBUILD_STATIONS = os.getenv("FORCE_REBUILD_STATIONS", "0") == "1"

# 「駅じゃない」誤爆を弾くワード
NON_STATION_WORDS = [
    "入口", "交番", "バス", "停留所", "公園", "小学校", "中学校", "高校",
    "病院", "郵便局", "市役所", "区役所", "図書館", "消防", "警察",
]

# =========================
# utils
# =========================
def safe_str(x: Any) -> str:
    return "" if x is None else str(x)

def norm_ws(s: Any) -> str:
    if s is None:
        return ""
    x = str(s).replace("　", " ")
    x = re.sub(r"\s+", " ", x)
    return x.strip()

def to_hira(s: str) -> str:
    # カタカナ→ひらがな
    return (s or "").translate(str.maketrans({chr(c): chr(c - 0x60) for c in range(0x30A1, 0x30F7)}))

def station_base(name: str) -> str:
    n = safe_str(name).strip()
    n = re.sub(r"\(.*?\)", "", n)
    n = re.sub(r"（.*?）", "", n)
    n = n.strip()
    if n.endswith("駅"):
        n = n[:-1]
    return n.strip()

def is_bad_station_name(name: str) -> bool:
    n = safe_str(name).strip()
    if n == "":
        return True
    if any(w in n for w in NON_STATION_WORDS):
        return True
    # 原則「駅」を含むべき（ただしGoogleが「新羽」みたいに返すことがあるので後段で補正）
    return False

def haversine_m(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    R = 6371000.0
    p1 = math.radians(lat1)
    p2 = math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lng2 - lng1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))

def sleep_api() -> None:
    if GOOGLE_API_SLEEP_SEC > 0:
        time.sleep(GOOGLE_API_SLEEP_SEC)

def parse_float(s: Any) -> Optional[float]:
    t = safe_str(s).strip()
    if not t:
        return None
    try:
        return float(t)
    except Exception:
        return None

# =========================
# Google API
# =========================
BASE = "https://maps.googleapis.com/maps/api"

def google_get(path: str, params: Dict[str, Any]) -> Dict[str, Any]:
    if not GOOGLE_MAPS_API_KEY:
        raise RuntimeError("GOOGLE_MAPS_API_KEY が未設定です")
    p = dict(params)
    p["key"] = GOOGLE_MAPS_API_KEY
    url = f"{BASE}/{path}"
    r = requests.get(url, params=p, timeout=60)
    r.raise_for_status()
    data = r.json()
    st = data.get("status")
    if st not in ("OK", "ZERO_RESULTS"):
        raise RuntimeError(f"Google API error: status={st} msg={data.get('error_message')}")
    return data

def places_textsearch(query: str) -> Optional[Dict[str, Any]]:
    sleep_api()
    data = google_get("place/textsearch/json", {"query": query, "language": "ja", "region": "jp"})
    res = data.get("results") or []
    return res[0] if res else None

def places_details(place_id: str) -> Optional[Dict[str, Any]]:
    sleep_api()
    fields = "name,formatted_address,geometry,types,formatted_phone_number,international_phone_number,website,url"
    data = google_get("place/details/json", {"place_id": place_id, "fields": fields, "language": "ja", "region": "jp"})
    return data.get("result")

def places_nearby_transit_station(lat: float, lng: float, radius_m: int) -> List[Dict[str, Any]]:
    sleep_api()
    data = google_get(
        "place/nearbysearch/json",
        {
            "location": f"{lat},{lng}",
            "radius": radius_m,
            "type": "transit_station",
            "language": "ja",
            "region": "jp",
        },
    )
    return data.get("results") or []

def distance_matrix_walk_minutes(orig_lat: float, orig_lng: float, dest_lat: float, dest_lng: float) -> Optional[int]:
    sleep_api()
    data = google_get(
        "distancematrix/json",
        {
            "origins": f"{orig_lat},{orig_lng}",
            "destinations": f"{dest_lat},{dest_lng}",
            "mode": "walking",
            "language": "ja",
            "region": "jp",
        },
    )
    rows = data.get("rows") or []
    if not rows:
        return None
    elems = rows[0].get("elements") or []
    if not elems:
        return None
    e0 = elems[0]
    if e0.get("status") != "OK":
        return None
    dur = (e0.get("duration") or {}).get("value")
    if dur is None:
        return None
    return int(round(float(dur) / 60.0))

# =========================
# master IO
# =========================
MASTER_FIELDS = [
    "facility_id",
    "name",
    "name_kana",
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
    "station_kana",
    "walk_minutes",
]

def read_master() -> List[Dict[str, str]]:
    if not MASTER_CSV.exists():
        raise RuntimeError(f"{MASTER_CSV} がありません")
    with MASTER_CSV.open("r", encoding="utf-8-sig", newline="") as f:
        rows = list(csv.DictReader(f))
    for r in rows:
        for k in MASTER_FIELDS:
            if k not in r:
                r[k] = ""
    return rows

def write_master(rows: List[Dict[str, str]]) -> None:
    with MASTER_CSV.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=MASTER_FIELDS)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in MASTER_FIELDS})

# =========================
# station cache (Yokohama-wide)
# =========================
def load_station_cache() -> Dict[str, Any]:
    if STATION_CACHE.exists() and not FORCE_REBUILD_STATIONS:
        try:
            return json.loads(STATION_CACHE.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}

def save_station_cache(cache: Dict[str, Any]) -> None:
    STATION_CACHE.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")

def build_yokohama_station_cache() -> Dict[str, Any]:
    """
    横浜市（＋WARD_FILTERがある場合は区）で駅をTextSearchで収集してキャッシュ化。
    収集は上限を持たせてコストを制御。
    """
    cache = load_station_cache()
    cache.setdefault("meta", {})
    cache.setdefault("stations", {})  # base -> {name, place_id, lat, lng}

    # すでに十分あれば再作成しない（forceの場合は先で削除される想定）
    if cache["stations"] and not FORCE_REBUILD_STATIONS:
        return cache

    # ★駅を集めるクエリ（広め→狭めの順）
    # 「横浜市 駅」で候補を取る（TextSearchの結果数は多いので上限で止める）
    q_parts = [CITY_FILTER, "駅"]
    if WARD_FILTER:
        q_parts.insert(1, WARD_FILTER)
    query = " ".join(q_parts)

    print("BUILD station cache query:", query)

    hit = places_textsearch(query)
    if not hit:
        # 最低限のフォールバック
        cache["meta"]["status"] = "seed_failed"
        save_station_cache(cache)
        return cache

    # TextSearch は1発で全部返らないので、厳密に全駅は拾えない。
    # ここは「失敗時の補完用」キャッシュなので、一定数拾えればOKという割り切り。
    # より完全にやるなら、鉄道会社別/路線別など追加クエリを積む拡張が可能。

    # まずは「横浜市 駅」からのトップ1のplace_idを詳細取得しておく（meta）
    cache["meta"]["seed_place_id"] = hit.get("place_id")
    cache["meta"]["query"] = query

    # 実際の駅は facilities の近傍検索で取れることが多いので、
    # キャッシュは「近傍検索が空振りした時の保険」として動けばOK。
    # よって、ここでは固定リストを持たず「施設処理の途中で出会った駅」を追加していく方式にする。

    cache["meta"]["status"] = "ok"
    save_station_cache(cache)
    return cache

def station_cache_upsert(cache: Dict[str, Any], name: str, place_id: Optional[str], lat: Optional[float], lng: Optional[float]) -> None:
    base = station_base(name)
    if not base:
        return
    cache.setdefault("stations", {})
    if base in cache["stations"]:
        # すでに座標が入ってるならそのまま
        if cache["stations"][base].get("lat") is not None and cache["stations"][base].get("lng") is not None:
            return
    cache["stations"][base] = {"name": name, "place_id": place_id, "lat": lat, "lng": lng}

def station_from_cache_nearest(cache: Dict[str, Any], lat: float, lng: float) -> Optional[Dict[str, Any]]:
    sts = cache.get("stations") or {}
    best = None
    best_d = 1e18
    for base, info in sts.items():
        slat = info.get("lat")
        slng = info.get("lng")
        if slat is None or slng is None:
            continue
        d = haversine_m(lat, lng, float(slat), float(slng))
        if d < best_d:
            best_d = d
            best = dict(info)
            best["base"] = base
            best["distance_m"] = best_d
    return best

# =========================
# station selection
# =========================
def pick_best_station_candidate(cands: List[Dict[str, Any]], orig_lat: float, orig_lng: float) -> Optional[Dict[str, Any]]:
    best = None
    best_d = 1e18
    for c in cands:
        name = safe_str(c.get("name")).strip()
        if is_bad_station_name(name):
            continue

        types = c.get("types") or []
        # 駅としてのtypeが無い場合は弱いので弾く（誤爆抑制）
        if not any(t in types for t in ("train_station", "subway_station", "transit_station")):
            # ただし「◯◯駅」なら残す
            if "駅" not in name:
                continue

        loc = ((c.get("geometry") or {}).get("location") or {})
        lat = loc.get("lat")
        lng = loc.get("lng")
        if lat is None or lng is None:
            continue

        d = haversine_m(orig_lat, orig_lng, float(lat), float(lng))
        if d < best_d:
            best_d = d
            best = dict(c)
            best["distance_m"] = best_d
    return best

def normalize_station_name(name: str) -> str:
    st_name = safe_str(name).strip()
    base = station_base(st_name)
    # Googleが「新羽」みたいに返しても「駅」を付ける
    if base and not st_name.endswith("駅"):
        st_name = base + "駅"
    # 「駅」が途中にあれば整形
    if "駅" in st_name and not st_name.endswith("駅"):
        st_name = st_name.split("駅")[0] + "駅"
    return st_name

def should_fix_station(row: Dict[str, str]) -> bool:
    st = safe_str(row.get("nearest_station")).strip()
    wk = safe_str(row.get("walk_minutes")).strip()
    if st == "" or wk == "":
        return True
    if is_bad_station_name(st):
        return True
    if "駅" not in st:
        return True
    return False

def strict_address_ok(addr: str, ward: str) -> bool:
    if not STRICT_ADDRESS_CHECK:
        return True
    a = safe_str(addr)
    if CITY_FILTER not in a:
        return False
    if ward and WARD_FILTER and WARD_FILTER not in a:
        return False
    return True

# =========================
# fix one row
# =========================
def fix_one_row(row: Dict[str, str], station_cache: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
    changed = False
    reason = None

    name = safe_str(row.get("name")).strip()
    ward = safe_str(row.get("ward")).strip()

    if WARD_FILTER and WARD_FILTER not in ward:
        return False, None

    # ---- facility details ----
    q = " ".join([name, CITY_FILTER, ward]).strip()
    q = re.sub(r"\s+", " ", q)

    hit = places_textsearch(q) or places_textsearch(f"{name} {CITY_FILTER}")
    if not (hit and hit.get("place_id")):
        return False, "facility_not_found"

    det = places_details(hit["place_id"])
    if not det:
        return False, "facility_details_failed"

    det_addr = safe_str(det.get("formatted_address")).strip()
    det_name = safe_str(det.get("name")).strip()
    types = det.get("types") or []
    phone = safe_str(det.get("formatted_phone_number") or det.get("international_phone_number")).strip()
    website = safe_str(det.get("website")).strip()
    map_url = safe_str(det.get("url")).strip()

    loc = ((det.get("geometry") or {}).get("location") or {})
    lat = loc.get("lat")
    lng = loc.get("lng")

    if det_addr and strict_address_ok(det_addr, ward):
        if safe_str(row.get("address")).strip() != det_addr:
            row["address"] = det_addr
            changed = True

    if lat is not None and lng is not None:
        if safe_str(row.get("lat")).strip() == "" or safe_str(row.get("lng")).strip() == "":
            row["lat"] = str(lat)
            row["lng"] = str(lng)
            changed = True

    if det_name and safe_str(row.get("name")).strip() == "":
        row["name"] = det_name
        changed = True

    if phone and (OVERWRITE_PHONE or safe_str(row.get("phone")).strip() == ""):
        row["phone"] = phone
        changed = True

    if website and (OVERWRITE_WEBSITE or safe_str(row.get("website")).strip() == ""):
        row["website"] = website
        changed = True

    if map_url and (OVERWRITE_MAP_URL or safe_str(row.get("map_url")).strip() == ""):
        row["map_url"] = map_url
        changed = True

    if types:
        t = ",".join(types)
        if safe_str(row.get("facility_type")).strip() != t:
            row["facility_type"] = t
            changed = True

    # ---- nearest station / walk ----
    if not FILL_NEAREST_STATION:
        return changed, None

    rlat = parse_float(row.get("lat"))
    rlng = parse_float(row.get("lng"))
    if rlat is None or rlng is None:
        return changed, "no_latlng"

    need_station = OVERWRITE_NEAREST_STATION or should_fix_station(row) or safe_str(row.get("nearest_station")).strip() == ""
    need_walk = OVERWRITE_WALK_MINUTES or safe_str(row.get("walk_minutes")).strip() == ""

    if not (need_station or need_walk):
        return changed, None

    # 1) Nearby transit_station
    cands = places_nearby_transit_station(rlat, rlng, NEARBY_RADIUS_M)
    st_place = pick_best_station_candidate(cands, rlat, rlng)

    # 近傍で駅が取れたらキャッシュに追加していく（全域化のキー）
    if st_place:
        st_name = normalize_station_name(st_place.get("name"))
        st_loc = ((st_place.get("geometry") or {}).get("location") or {})
        station_cache_upsert(
            station_cache,
            st_name,
            st_place.get("place_id"),
            st_loc.get("lat"),
            st_loc.get("lng"),
        )
    else:
        # 2) cache fallback
        st_place = station_from_cache_nearest(station_cache, rlat, rlng)

    if not st_place:
        return changed, "station_not_found"

    # 駅名
    st_name = normalize_station_name(st_place.get("name"))

    if need_station:
        if OVERWRITE_NEAREST_STATION or safe_str(row.get("nearest_station")).strip() == "" or should_fix_station(row):
            row["nearest_station"] = st_name
            changed = True

        # kana（最低限：駅名ベースをひらがな化）
        base = station_base(st_name)
        if safe_str(row.get("station_kana")).strip() == "":
            row["station_kana"] = to_hira(base)
            changed = True

    if need_walk:
        # destination latlng
        dlat = None
        dlng = None
        if st_place.get("geometry"):
            dloc = ((st_place.get("geometry") or {}).get("location") or {})
            dlat = dloc.get("lat")
            dlng = dloc.get("lng")
        if dlat is None or dlng is None:
            dlat = st_place.get("lat")
            dlng = st_place.get("lng")

        if dlat is not None and dlng is not None:
            mins = distance_matrix_walk_minutes(rlat, rlng, float(dlat), float(dlng))
            if mins is not None:
                row["walk_minutes"] = str(int(mins))
                changed = True
            else:
                reason = "walk_failed"
        else:
            reason = "station_no_latlng"

    return changed, reason

# =========================
# main
# =========================
def main() -> None:
    if not GOOGLE_MAPS_API_KEY:
        raise SystemExit("ERROR: GOOGLE_MAPS_API_KEY is empty.")

    print("START fix_master_with_google_places.py")
    print("CITY_FILTER =", CITY_FILTER, "WARD_FILTER =", WARD_FILTER or "(all wards)")
    print("ONLY_BAD_ROWS =", ONLY_BAD_ROWS, "MAX_UPDATES =", MAX_UPDATES)
    print("NEARBY_RADIUS_M =", NEARBY_RADIUS_M)
    print("FORCE_REBUILD_STATIONS =", FORCE_REBUILD_STATIONS)

    station_cache = build_yokohama_station_cache()
    rows = read_master()

    updates = 0
    misses: List[Dict[str, str]] = []

    for r in rows:
        if updates >= MAX_UPDATES:
            break

        # 対象区だけ
        if WARD_FILTER and WARD_FILTER not in safe_str(r.get("ward")):
            continue

        if ONLY_BAD_ROWS:
            addr_empty = safe_str(r.get("address")).strip() == ""
            ll_empty = safe_str(r.get("lat")).strip() == "" or safe_str(r.get("lng")).strip() == ""
            st_bad = should_fix_station(r)
            if not (addr_empty or ll_empty or st_bad):
                continue

        changed, reason = fix_one_row(r, station_cache)
        if changed:
            updates += 1
        if reason:
            misses.append(
                {
                    "facility_id": safe_str(r.get("facility_id")).strip(),
                    "name": safe_str(r.get("name")).strip(),
                    "ward": safe_str(r.get("ward")).strip(),
                    "reason": reason,
                }
            )

    # キャッシュを保存（施設処理で駅を学習して増えていく）
    save_station_cache(station_cache)

    write_master(rows)
    print("DONE. wrote:", str(MASTER_CSV))
    print("updated rows:", updates, "/", len(rows))
    print("station cache size:", len((station_cache.get("stations") or {}).keys()))

    # misses
    with STATION_MISSES.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["facility_id", "name", "ward", "reason"])
        w.writeheader()
        for m in misses:
            w.writerow(m)
    print("wrote:", str(STATION_MISSES), "count:", len(misses))
    print("OK")

if __name__ == "__main__":
    main()
