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
from pykakasi import kakasi

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

MASTER_CSV = DATA_DIR / "master_facilities.csv"
STATIONS_CACHE = DATA_DIR / "stations_cache_yokohama.json"
STATION_MISSES = DATA_DIR / "station_misses.csv"

API_KEY = (os.getenv("GOOGLE_MAPS_API_KEY", "") or "").strip()
if not API_KEY:
    raise RuntimeError("GOOGLE_MAPS_API_KEY が未設定です（GitHub Secrets を確認）")

CITY_FILTER = (os.getenv("CITY_FILTER", "横浜市") or "").strip()  # 将来: 横浜市以外にも対応可能
WARD_FILTER = (os.getenv("WARD_FILTER", "") or "").strip() or None

MAX_UPDATES = int(os.getenv("MAX_UPDATES", "200"))
ONLY_BAD_ROWS = (os.getenv("ONLY_BAD_ROWS", "0") == "1")
STRICT_ADDRESS_CHECK = (os.getenv("STRICT_ADDRESS_CHECK", "1") == "1")
SLEEP_SEC = float(os.getenv("GOOGLE_API_SLEEP_SEC", "0.15"))

OVERWRITE_PHONE = (os.getenv("OVERWRITE_PHONE", "0") == "1")
OVERWRITE_WEBSITE = (os.getenv("OVERWRITE_WEBSITE", "0") == "1")
OVERWRITE_MAP_URL = (os.getenv("OVERWRITE_MAP_URL", "0") == "1")

FILL_NEAREST_STATION = (os.getenv("FILL_NEAREST_STATION", "1") == "1")
OVERWRITE_NEAREST_STATION = (os.getenv("OVERWRITE_NEAREST_STATION", "0") == "1")
OVERWRITE_WALK_MINUTES = (os.getenv("OVERWRITE_WALK_MINUTES", "0") == "1")

NEARBY_RADIUS_M = int(os.getenv("NEARBY_RADIUS_M", "2500"))
FORCE_REBUILD_STATIONS = (os.getenv("FORCE_REBUILD_STATIONS", "0") == "1")

SESSION = requests.Session()

# -------------------------
# Utils
# -------------------------
def safe(s: Any) -> str:
    return "" if s is None else str(s)

def norm_spaces(s: str) -> str:
    s = safe(s).replace("　", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s

def norm_compact(s: str) -> str:
    s = safe(s).replace("　", " ")
    s = re.sub(r"\s+", "", s).strip()
    return s

def to_int(x: Any) -> Optional[int]:
    s = safe(x).strip()
    if not s:
        return None
    try:
        return int(float(s))
    except Exception:
        return None

def haversine_m(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    R = 6371000.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lng2 - lng1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return 2 * R * math.atan2(math.sqrt(a), math.sqrt(1 - a))

def walk_minutes_from_distance(distance_m: float) -> int:
    # 平地: 80m/分 をデフォルト（だいたい時速4.8km）
    return max(1, int(round(distance_m / 80.0)))

def sleep():
    if SLEEP_SEC > 0:
        time.sleep(SLEEP_SEC)

# -------------------------
# Kana conversion
# -------------------------
_kks = kakasi()
_kks.setMode("J", "H")  # Kanji -> Hiragana (approx)
_kks.setMode("K", "H")  # Katakana -> Hiragana
_kks.setMode("H", "H")
_conv = _kks.getConverter()

def to_hiragana(s: str) -> str:
    s = norm_spaces(s)
    if not s:
        return ""
    return _conv.do(s)

def normalize_station_name(name: str) -> str:
    """
    - "日吉" -> "日吉駅"
    - "日吉駅（東急）" -> "日吉駅"
    - "Shin-Yokohama Station" みたいなのが来たらそのまま（日本語想定）
    """
    n = norm_spaces(name)
    if not n:
        return ""
    n = re.sub(r"[（(].*?[）)]", "", n).strip()
    n = re.sub(r"\s+Station$", "", n, flags=re.I).strip()
    # 末尾に駅が無ければ付ける（ただし「駅前」などは除外）
    if not n.endswith("駅"):
        n = n + "駅"
    # "駅駅" を潰す
    n = n.replace("駅駅", "駅")
    return n

def is_stationish(place_name: str, types: List[str], vicinity: str = "") -> bool:
    """
    駅以外（コンビニ/学校/役所等）を弾くための判定。
    Placesのtypeが transit_station / train_station / subway_station などを強く信頼する。
    """
    name = norm_spaces(place_name)
    vic = norm_spaces(vicinity)
    tset = set(types or [])
    # type で強判定
    if ("transit_station" in tset) or ("train_station" in tset) or ("subway_station" in tset) or ("light_rail_station" in tset):
        return True
    # 名前で弱判定（末尾が駅 or "Station"）
    if name.endswith("駅") or re.search(r"\bStation\b", name, flags=re.I):
        # 駅前/駅入口などの施設は除外したいので、末尾が駅でないなら弱い
        if name.endswith("駅"):
            return True
    # vicinity に駅がある程度入っているか
    if ("駅" in name) or ("駅" in vic):
        # ただし "駅前" は駅そのものではない
        if "駅前" in name and not name.endswith("駅"):
            return False
    return False

# -------------------------
# Google APIs
# -------------------------
def g_get(url: str, params: Dict[str, Any]) -> Dict[str, Any]:
    r = SESSION.get(url, params=params, timeout=60)
    r.raise_for_status()
    data = r.json()
    status = data.get("status")
    if status not in (None, "OK", "ZERO_RESULTS"):
        # REQUEST_DENIED / OVER_QUERY_LIMIT / INVALID_REQUEST etc
        raise RuntimeError(f"Google API error: status={status} msg={data.get('error_message')}")
    return data

def places_text_search(query: str) -> Optional[Dict[str, Any]]:
    url = "https://maps.googleapis.com/maps/api/place/textsearch/json"
    params = {"query": query, "key": API_KEY, "language": "ja"}
    data = g_get(url, params)
    results = data.get("results", [])
    if not results:
        return None
    return results[0]

def place_details(place_id: str) -> Dict[str, Any]:
    url = "https://maps.googleapis.com/maps/api/place/details/json"
    fields = ",".join([
        "place_id", "name", "formatted_address", "geometry/location",
        "types", "formatted_phone_number", "website", "url"
    ])
    params = {"place_id": place_id, "fields": fields, "key": API_KEY, "language": "ja"}
    data = g_get(url, params)
    return data.get("result", {}) or {}

def nearby_transit_stations(lat: float, lng: float, radius_m: int) -> List[Dict[str, Any]]:
    """
    駅だけを取りたいので type=transit_station を固定。
    """
    url = "https://maps.googleapis.com/maps/api/place/nearbysearch/json"
    params = {
        "location": f"{lat},{lng}",
        "radius": radius_m,
        "type": "transit_station",
        "key": API_KEY,
        "language": "ja",
    }
    data = g_get(url, params)
    return data.get("results", []) or []

def distance_matrix_walk_minutes(origin_lat: float, origin_lng: float, dest_lat: float, dest_lng: float) -> Optional[int]:
    """
    徒歩時間：Distance Matrix を使えるならそれが最優先。
    """
    url = "https://maps.googleapis.com/maps/api/distancematrix/json"
    params = {
        "origins": f"{origin_lat},{origin_lng}",
        "destinations": f"{dest_lat},{dest_lng}",
        "mode": "walking",
        "key": API_KEY,
        "language": "ja",
    }
    data = g_get(url, params)
    rows = data.get("rows", [])
    if not rows:
        return None
    elems = rows[0].get("elements", [])
    if not elems:
        return None
    el = elems[0]
    if el.get("status") != "OK":
        return None
    dur = el.get("duration", {}).get("value")  # seconds
    if dur is None:
        return None
    return max(1, int(round(float(dur) / 60.0)))

# -------------------------
# CSV load/save
# -------------------------
CSV_HEADER = [
    "facility_id","name","ward","address","lat","lng",
    "facility_type","phone","website","notes",
    "nearest_station","walk_minutes",
    "name_kana","station_kana",
    "map_url"
]

def read_master_rows() -> List[Dict[str, str]]:
    if not MASTER_CSV.exists():
        raise RuntimeError("data/master_facilities.csv がありません")
    with MASTER_CSV.open("r", encoding="utf-8-sig", newline="") as f:
        rows = list(csv.DictReader(f))
    # 欠けている列があっても落ちないように補完
    for r in rows:
        for k in CSV_HEADER:
            if k not in r:
                r[k] = ""
    return rows

def write_master_rows(rows: List[Dict[str, str]]) -> None:
    with MASTER_CSV.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=CSV_HEADER)
        w.writeheader()
        for r in rows:
            w.writerow({k: safe(r.get(k, "")) for k in CSV_HEADER})

# -------------------------
# Station cache (optional)
# -------------------------
def load_station_cache() -> Dict[str, Any]:
    if FORCE_REBUILD_STATIONS:
        return {}
    if STATIONS_CACHE.exists():
        try:
            return json.loads(STATIONS_CACHE.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}

def save_station_cache(cache: Dict[str, Any]) -> None:
    STATIONS_CACHE.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")

# -------------------------
# Place -> fill fields
# -------------------------
def is_bad_station_value(s: str) -> bool:
    """
    「駅じゃない」値を bad とみなす（例: 保育園名/施設名/丁目など）。
    """
    t = norm_spaces(s)
    if not t:
        return True
    # 明らかに駅じゃない
    if not t.endswith("駅"):
        return True
    # 「〇〇駅前」みたいなのが来たら駅ではない（駅そのものに寄せたい）
    if "駅前" in t and not t.endswith("駅"):
        return True
    return False

def strict_address_ok(addr: str) -> bool:
    """
    STRICT_ADDRESS_CHECK=1 の場合：
    - CITY_FILTER を含む
    - WARD_FILTER 指定がある場合は区名も含む
    """
    a = norm_compact(addr)
    if not a:
        return False
    if CITY_FILTER and (CITY_FILTER not in a):
        return False
    if WARD_FILTER:
        if norm_compact(WARD_FILTER) not in a:
            return False
    return True

def find_place_for_facility(name: str, ward: str, address_hint: str) -> Optional[Dict[str, Any]]:
    # 検索クエリ：名前 + 市 + 区(あれば) + 日本 で寄せる
    parts = [name]
    if CITY_FILTER:
        parts.append(CITY_FILTER)
    if ward:
        parts.append(ward)
    if address_hint:
        parts.append(address_hint)
    parts.append("日本")
    query = " ".join([p for p in parts if p]).strip()
    query = re.sub(r"\s+", " ", query)
    res = places_text_search(query)
    return res

def pick_best_station_for(lat: float, lng: float, cache: Dict[str, Any]) -> Tuple[str, Optional[int], str]:
    """
    returns: (station_name, walk_minutes, debug_reason)
    """
    cache_key = f"{lat:.6f},{lng:.6f},{NEARBY_RADIUS_M}"
    if cache_key in cache:
        c = cache[cache_key]
        return c.get("nearest_station",""), c.get("walk_minutes"), "cache"

    candidates = nearby_transit_stations(lat, lng, NEARBY_RADIUS_M)
    sleep()

    scored: List[Tuple[float, Dict[str, Any]]] = []
    for c in candidates:
        name = safe(c.get("name"))
        types = c.get("types") or []
        vic = safe(c.get("vicinity"))
        if not is_stationish(name, types, vic):
            continue

        gloc = c.get("geometry", {}).get("location", {})
        slat = gloc.get("lat")
        slng = gloc.get("lng")
        if slat is None or slng is None:
            continue

        d = haversine_m(lat, lng, float(slat), float(slng))

        # スコア：距離 + 「駅っぽさ」ボーナス
        score = d
        nm = normalize_station_name(name)
        if nm.endswith("駅"):
            score -= 80  # 少し優遇
        if "駅前" in name:
            score += 500  # 駅前施設を落とす方向
        scored.append((score, {**c, "_dist_m": d, "_norm_name": nm, "_slat": float(slat), "_slng": float(slng)}))

    scored.sort(key=lambda x: x[0])

    if not scored:
        cache[cache_key] = {"nearest_station": "", "walk_minutes": None}
        return "", None, "no_station_candidates"

    best = scored[0][1]
    station_name = best["_norm_name"]

    # 徒歩分数：Distance Matrix 優先、失敗したら距離から概算
    wm = None
    try:
        wm = distance_matrix_walk_minutes(lat, lng, best["_slat"], best["_slng"])
        sleep()
    except Exception:
        wm = None
    if wm is None:
        wm = walk_minutes_from_distance(best["_dist_m"])

    cache[cache_key] = {"nearest_station": station_name, "walk_minutes": wm}
    return station_name, wm, "nearby+score"

# -------------------------
# Main
# -------------------------
def main():
    print("START fix_master_with_google_places.py")
    print("CITY_FILTER=", CITY_FILTER, "WARD_FILTER=", WARD_FILTER)
    print("MAX_UPDATES=", MAX_UPDATES, "ONLY_BAD_ROWS=", ONLY_BAD_ROWS)

    rows = read_master_rows()
    cache = load_station_cache()

    # misses log
    misses: List[Dict[str, str]] = []

    updated = 0
    for r in rows:
        fid = safe(r.get("facility_id")).strip()
        name = norm_spaces(r.get("name",""))
        ward = norm_spaces(r.get("ward",""))
        address = norm_spaces(r.get("address",""))
        lat_s = safe(r.get("lat")).strip()
        lng_s = safe(r.get("lng")).strip()

        if not fid or not name:
            continue

        # フィルタ（横浜市全域でも ward は master に入ってる前提）
        if WARD_FILTER and (WARD_FILTER not in ward):
            continue

        # ONLY_BAD_ROWS: 駅が空 or 駅っぽくない or 徒歩が空/不正 の行だけ
        if ONLY_BAD_ROWS:
            st = norm_spaces(r.get("nearest_station",""))
            wm = safe(r.get("walk_minutes","")).strip()
            bad_station = is_bad_station_value(st)
            bad_walk = (wm == "" or to_int(wm) is None)
            if not (bad_station or bad_walk or not address or not lat_s or not lng_s):
                continue

        # すでに十分埋まっていて、上書きしない設定ならスキップ（駅も同様）
        # ただし ONLY_BAD_ROWS=0 のときは空欄埋めを狙う
        # Places で補完したい場合に備え、lat/lng/address が無いなら place を引く

        place = None
        if (not address) or (not lat_s) or (not lng_s) or OVERWRITE_MAP_URL or OVERWRITE_PHONE or OVERWRITE_WEBSITE:
            place = find_place_for_facility(name=name, ward=ward, address_hint=address)
            sleep()

        if place:
            pid = place.get("place_id")
            if pid:
                det = place_details(pid)
                sleep()

                fmt_addr = safe(det.get("formatted_address"))
                if fmt_addr:
                    if (not address) or (not STRICT_ADDRESS_CHECK) or strict_address_ok(fmt_addr):
                        r["address"] = fmt_addr

                gloc = det.get("geometry", {}).get("location", {})
                if gloc.get("lat") is not None and gloc.get("lng") is not None:
                    r["lat"] = str(gloc.get("lat"))
                    r["lng"] = str(gloc.get("lng"))

                types = det.get("types") or place.get("types") or []
                r["facility_type"] = ",".join(types) if types else safe(r.get("facility_type",""))

                phone = safe(det.get("formatted_phone_number"))
                if phone and (OVERWRITE_PHONE or not safe(r.get("phone")).strip()):
                    r["phone"] = phone

                website = safe(det.get("website"))
                if website and (OVERWRITE_WEBSITE or not safe(r.get("website")).strip()):
                    r["website"] = website

                url = safe(det.get("url"))
                if url and (OVERWRITE_MAP_URL or not safe(r.get("map_url")).strip()):
                    # url は長いことがあるが確実に map を開ける
                    r["map_url"] = url

        # name_kana（空なら生成）
        if not safe(r.get("name_kana","")).strip():
            r["name_kana"] = to_hiragana(name)

        # 駅/徒歩
        if FILL_NEAREST_STATION:
            lat_s = safe(r.get("lat")).strip()
            lng_s = safe(r.get("lng")).strip()
            if lat_s and lng_s:
                try:
                    lat = float(lat_s)
                    lng = float(lng_s)
                    cur_station = norm_spaces(r.get("nearest_station",""))
                    cur_walk = safe(r.get("walk_minutes","")).strip()
                    need_station = (not cur_station) or is_bad_station_value(cur_station) or OVERWRITE_NEAREST_STATION
                    need_walk = (cur_walk == "" or to_int(cur_walk) is None or OVERWRITE_WALK_MINUTES)

                    if need_station or need_walk:
                        stname, wmin, reason = pick_best_station_for(lat, lng, cache)
                        if stname:
                            if need_station:
                                r["nearest_station"] = stname
                            if need_walk and (wmin is not None):
                                r["walk_minutes"] = str(int(wmin))
                            # station_kana
                            if stname and (OVERWRITE_NEAREST_STATION or not safe(r.get("station_kana","")).strip()):
                                r["station_kana"] = to_hiragana(stname.replace("駅",""))
                        else:
                            misses.append({
                                "facility_id": fid,
                                "name": name,
                                "ward": ward,
                                "reason": reason,
                                "lat": lat_s,
                                "lng": lng_s,
                            })

                except Exception as e:
                    misses.append({
                        "facility_id": fid,
                        "name": name,
                        "ward": ward,
                        "reason": f"exception: {e}",
                        "lat": lat_s,
                        "lng": lng_s,
                    })

        updated += 1
        if updated >= MAX_UPDATES:
            break

    # save cache + master
    save_station_cache(cache)
    write_master_rows(rows)

    # misses
    if misses:
        with STATION_MISSES.open("w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=["facility_id","name","ward","reason","lat","lng"])
            w.writeheader()
            for m in misses:
                w.writerow(m)

    print("DONE.")
    print("updated rows:", updated)
    print("station misses:", len(misses))
    print("wrote:", str(MASTER_CSV))
    print("wrote cache:", str(STATIONS_CACHE))
    if misses:
        print("wrote misses:", str(STATION_MISSES))


if __name__ == "__main__":
    main()
