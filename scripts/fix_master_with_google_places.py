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

# ========= Config =========
API_KEY = (os.getenv("GOOGLE_MAPS_API_KEY") or "").strip()

WARD_FILTER = (os.getenv("WARD_FILTER", "港北区") or "").strip() or None
MAX_UPDATES = int(os.getenv("MAX_UPDATES", "80"))
ONLY_BAD_ROWS = (os.getenv("ONLY_BAD_ROWS", "1") == "1")
STRICT_ADDRESS_CHECK = (os.getenv("STRICT_ADDRESS_CHECK", "1") == "1")
SLEEP_SEC = float(os.getenv("GOOGLE_API_SLEEP_SEC", "0.15"))

OVERWRITE_PHONE = (os.getenv("OVERWRITE_PHONE", "0") == "1")
OVERWRITE_WEBSITE = (os.getenv("OVERWRITE_WEBSITE", "0") == "1")
OVERWRITE_MAP_URL = (os.getenv("OVERWRITE_MAP_URL", "0") == "1")

FILL_NEAREST_STATION = (os.getenv("FILL_NEAREST_STATION", "1") == "1")
OVERWRITE_NEAREST_STATION = (os.getenv("OVERWRITE_NEAREST_STATION", "1") == "1")
OVERWRITE_WALK_MINUTES = (os.getenv("OVERWRITE_WALK_MINUTES", "1") == "1")

# 港北区＋周辺（境界で最寄りになりがちな駅も含める）
KOHOKU_STATIONS = [
    "新横浜駅", "北新横浜駅", "新羽駅", "高田駅", "日吉本町駅", "日吉駅",
    "綱島駅", "新綱島駅",
    "菊名駅", "大倉山駅", "小机駅",
    # 境界補完（外れ値対策：近いのに港北区外で落ちるケース）
    "妙蓮寺駅", "白楽駅", "反町駅",  # 神奈川区寄り
    "武蔵小杉駅", "元住吉駅"         # 日吉・綱島の南側寄り
]

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)

MASTER_CSV = DATA_DIR / "master_facilities.csv"
CACHE_STATIONS = DATA_DIR / "kohoku_stations_cache.json"
MISSES_CSV = DATA_DIR / "station_misses.csv"

PLACES_TEXT = "https://maps.googleapis.com/maps/api/place/textsearch/json"
PLACES_DETAILS = "https://maps.googleapis.com/maps/api/place/details/json"
PLACES_NEARBY = "https://maps.googleapis.com/maps/api/place/nearbysearch/json"
DIST_MATRIX = "https://maps.googleapis.com/maps/api/distancematrix/json"

# ========= Kana converter =========
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


def safe_str(x: Any) -> str:
    return "" if x is None else str(x)


def to_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        s = str(x).strip()
        if s == "" or s.lower() == "nan":
            return None
        return float(s)
    except Exception:
        return None


def to_int(x: Any) -> Optional[int]:
    try:
        if x is None:
            return None
        s = str(x).strip()
        if s == "" or s.lower() == "nan":
            return None
        return int(float(s))
    except Exception:
        return None


def sleep():
    if SLEEP_SEC > 0:
        time.sleep(SLEEP_SEC)


def google_get(url: str, params: Dict[str, Any]) -> Dict[str, Any]:
    r = requests.get(url, params=params, timeout=60)
    r.raise_for_status()
    return r.json()


def ensure_cols(row: Dict[str, str], cols: List[str]) -> None:
    for c in cols:
        if c not in row:
            row[c] = ""


def load_csv_rows(path: Path) -> Tuple[List[str], List[Dict[str, str]]]:
    if not path.exists():
        raise RuntimeError(f"Not found: {path}")
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        header = reader.fieldnames or []
        rows = [dict(r) for r in reader]
    return header, rows


def write_csv_rows(path: Path, header: List[str], rows: List[Dict[str, str]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=header)
        w.writeheader()
        for r in rows:
            w.writerow(r)


def station_base(st: str) -> str:
    st = (st or "").strip()
    if st.endswith("駅"):
        st = st[:-1].strip()
    return st


def is_station_name(name: str) -> bool:
    name = (name or "").strip()
    if not name:
        return False
    # 駅を含み、バス停っぽいものは除外
    if "バス" in name or "停" in name and ("駅" not in name):
        return False
    return ("駅" in name)


def is_good_station_candidate(place: Dict[str, Any]) -> bool:
    name = safe_str(place.get("name")).strip()
    if not is_station_name(name):
        return False

    types = place.get("types") or []
    # bus_station を弾く
    if "bus_station" in types:
        return False

    # 駅系タイプを優先（Placesのタイプは揺れるので広めに許可）
    ok_types = {"train_station", "subway_station", "transit_station", "light_rail_station"}
    if any(t in ok_types for t in types):
        return True

    # types が薄い/欠けるケースの保険：nameが「◯◯駅」なら許可
    if name.endswith("駅"):
        return True

    return False


def haversine_m(lat1, lon1, lat2, lon2) -> float:
    R = 6371000.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat/2)**2 + math.cos(p1)*math.cos(p2)*math.sin(dlon/2)**2
    return 2*R*math.asin(math.sqrt(a))


def load_station_cache() -> Dict[str, Dict[str, Any]]:
    if CACHE_STATIONS.exists():
        try:
            return json.loads(CACHE_STATIONS.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}


def save_station_cache(cache: Dict[str, Dict[str, Any]]) -> None:
    CACHE_STATIONS.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")


def places_textsearch(query: str) -> Optional[Dict[str, Any]]:
    if not API_KEY:
        raise RuntimeError("GOOGLE_MAPS_API_KEY is empty")
    params = {"query": query, "key": API_KEY, "language": "ja", "region": "jp"}
    js = google_get(PLACES_TEXT, params)
    sleep()
    if js.get("status") != "OK":
        return None
    results = js.get("results") or []
    return results[0] if results else None


def places_details(place_id: str) -> Optional[Dict[str, Any]]:
    if not API_KEY:
        raise RuntimeError("GOOGLE_MAPS_API_KEY is empty")
    fields = ",".join([
        "place_id", "name", "formatted_address", "geometry", "types",
        "formatted_phone_number", "website", "url"
    ])
    params = {"place_id": place_id, "fields": fields, "key": API_KEY, "language": "ja", "region": "jp"}
    js = google_get(PLACES_DETAILS, params)
    sleep()
    if js.get("status") != "OK":
        return None
    return js.get("result")


def nearby_stations(lat: float, lng: float) -> Optional[Dict[str, Any]]:
    """
    まず NearbySearch(rankby=distance) で駅を取りに行く。
    ダメなら None。
    """
    if not API_KEY:
        raise RuntimeError("GOOGLE_MAPS_API_KEY is empty")

    location = f"{lat},{lng}"

    # typeを段階的に試す（train/subway/transit）
    for tp in ["train_station", "subway_station", "transit_station"]:
        params = {
            "location": location,
            "rankby": "distance",
            "type": tp,
            "keyword": "駅",
            "key": API_KEY,
            "language": "ja",
            "region": "jp",
        }
        js = google_get(PLACES_NEARBY, params)
        sleep()
        if js.get("status") not in ("OK", "ZERO_RESULTS"):
            continue
        results = js.get("results") or []
        for r in results[:10]:
            if is_good_station_candidate(r):
                return r

    return None


def station_from_list(lat: float, lng: float) -> Optional[Dict[str, Any]]:
    """
    港北区駅リストへフォールバック。
    1) 駅ごとにplace_id/lat/lngをキャッシュ
    2) 直線距離で最短候補を選ぶ
    """
    cache = load_station_cache()
    changed = False

    candidates: List[Tuple[float, Dict[str, Any]]] = []

    for st in KOHOKU_STATIONS:
        if st not in cache:
            hit = places_textsearch(f"{st} 神奈川県 横浜市")
            if hit and hit.get("place_id"):
                det = places_details(hit["place_id"])
                if det and det.get("geometry", {}).get("location"):
                    loc = det["geometry"]["location"]
                    cache[st] = {
                        "name": det.get("name") or st,
                        "place_id": det.get("place_id"),
                        "lat": loc.get("lat"),
                        "lng": loc.get("lng"),
                        "types": det.get("types") or [],
                        "url": det.get("url") or "",
                    }
                    changed = True
            sleep()

        d = cache.get(st)
        if not d:
            continue
        slat = to_float(d.get("lat"))
        slng = to_float(d.get("lng"))
        if slat is None or slng is None:
            continue

        dist = haversine_m(lat, lng, slat, slng)
        candidates.append((dist, d))

    if changed:
        save_station_cache(cache)

    if not candidates:
        return None

    candidates.sort(key=lambda x: x[0])
    return candidates[0][1]


def distance_matrix_walk_minutes(origin_lat: float, origin_lng: float, dest_place_id: str) -> Optional[int]:
    """
    徒歩時間（分）をDistance Matrixで取得。
    取れなければNone。
    """
    if not API_KEY:
        raise RuntimeError("GOOGLE_MAPS_API_KEY is empty")

    params = {
        "origins": f"{origin_lat},{origin_lng}",
        "destinations": f"place_id:{dest_place_id}",
        "mode": "walking",
        "language": "ja",
        "region": "jp",
        "key": API_KEY,
    }
    js = google_get(DIST_MATRIX, params)
    sleep()

    if js.get("status") != "OK":
        return None

    rows = js.get("rows") or []
    if not rows:
        return None
    els = (rows[0].get("elements") or [])
    if not els:
        return None
    e0 = els[0]
    if e0.get("status") != "OK":
        return None

    sec = (e0.get("duration") or {}).get("value")
    if sec is None:
        return None
    return int(math.ceil(float(sec) / 60.0))


def should_fix_station(row: Dict[str, str]) -> bool:
    st = safe_str(row.get("nearest_station")).strip()
    wm = to_int(row.get("walk_minutes"))
    if st == "" or not st.endswith("駅"):
        return True
    if wm is None or wm <= 0 or wm >= 200:
        return True
    return False


def should_fix_core(row: Dict[str, str]) -> bool:
    # 住所 or lat/lng が無いなら優先修正対象
    addr = safe_str(row.get("address")).strip()
    lat = to_float(row.get("lat"))
    lng = to_float(row.get("lng"))
    return (addr == "" or lat is None or lng is None)


def strict_address_ok(addr: str) -> bool:
    if not STRICT_ADDRESS_CHECK:
        return True
    if not addr:
        return False
    if "横浜市" not in addr:
        return False
    if WARD_FILTER and WARD_FILTER not in addr:
        return False
    return True


def fix_one_row(row: Dict[str, str], misses: List[Dict[str, str]]) -> bool:
    """
    1行を修正。何か変更したら True。
    """
    changed = False

    fid = safe_str(row.get("facility_id")).strip()
    name = safe_str(row.get("name")).strip()
    ward = safe_str(row.get("ward")).strip()

    # kana（空なら生成）
    if safe_str(row.get("name_kana")).strip() == "":
        row["name_kana"] = hira(name)
        changed = True

    # まず園自体のPlacesを引く（住所/緯度経度の安定化）
    lat = to_float(row.get("lat"))
    lng = to_float(row.get("lng"))

    if API_KEY and (should_fix_core(row) or (OVERWRITE_MAP_URL is False and safe_str(row.get("map_url")).strip() == "")):
        q = f"{name} 横浜市{ward} 保育園"
        hit = places_textsearch(q)
        if not hit or not hit.get("place_id"):
            misses.append({"facility_id": fid, "name": name, "ward": ward, "reason": "place_not_found", "query": q})
            return changed

        det = places_details(hit["place_id"])
        if not det:
            misses.append({"facility_id": fid, "name": name, "ward": ward, "reason": "place_details_failed", "query": q})
            return changed

        addr = safe_str(det.get("formatted_address")).strip()
        if addr and strict_address_ok(addr):
            if safe_str(row.get("address")).strip() == "" or safe_str(row.get("address")).strip() != addr:
                row["address"] = addr
                changed = True

        loc = (det.get("geometry") or {}).get("location") or {}
        dlat = loc.get("lat")
        dlng = loc.get("lng")
        if dlat is not None and dlng is not None:
            if safe_str(row.get("lat")).strip() == "" or abs(float(dlat) - (lat or 0.0)) > 1e-9:
                row["lat"] = str(dlat)
                changed = True
            if safe_str(row.get("lng")).strip() == "" or abs(float(dlng) - (lng or 0.0)) > 1e-9:
                row["lng"] = str(dlng)
                changed = True
            lat, lng = float(dlat), float(dlng)

        types = det.get("types") or []
        if types:
            if safe_str(row.get("facility_type")).strip() == "":
                row["facility_type"] = ",".join(types)
                changed = True

        phone = safe_str(det.get("formatted_phone_number")).strip()
        if phone and (OVERWRITE_PHONE or safe_str(row.get("phone")).strip() == ""):
            row["phone"] = phone
            changed = True

        website = safe_str(det.get("website")).strip()
        if website and (OVERWRITE_WEBSITE or safe_str(row.get("website")).strip() == ""):
            row["website"] = website
            changed = True

        map_url = safe_str(det.get("url")).strip()
        if map_url and (OVERWRITE_MAP_URL or safe_str(row.get("map_url")).strip() == ""):
            row["map_url"] = map_url
            changed = True

    # ===== 駅＋徒歩 =====
    if FILL_NEAREST_STATION and lat is not None and lng is not None:
        if (not ONLY_BAD_ROWS) or should_fix_station(row) or OVERWRITE_NEAREST_STATION or OVERWRITE_WALK_MINUTES:
            # 1) Nearbyで駅を探す（駅だけフィルタ）
            st_place = nearby_stations(lat, lng)

            # 2) ダメなら港北駅リストから距離で当てる
            if not st_place:
                st_place = station_from_list(lat, lng)

            if not st_place:
                misses.append({"facility_id": fid, "name": name, "ward": ward, "reason": "station_not_found", "query": f"{name} {ward}"})
                return changed

            st_name = safe_str(st_place.get("name")).strip()
            st_pid = safe_str(st_place.get("place_id")).strip()
            # リスト由来のdictは keys が揃ってるが、念のため
            if st_pid == "":
                # nearby結果のplace_idが無いケース対策：テキスト検索
                hit = places_textsearch(f"{st_name} 横浜市")
                if hit and hit.get("place_id"):
                    st_pid = hit["place_id"]

            if st_name:
                if OVERWRITE_NEAREST_STATION or safe_str(row.get("nearest_station")).strip() == "" or should_fix_station(row):
                    # “◯◯駅(…)" を “◯◯駅” に寄せる
                    if "駅" in st_name and not st_name.endswith("駅"):
                        st_name = st_name.split("駅")[0] + "駅"
                    row["nearest_station"] = st_name
                    changed = True

                # kana（駅名は "駅" を外してひらがな化）
                base = station_base(st_name)
                if safe_str(row.get("station_kana")).strip() == "":
                    row["station_kana"] = hira(base)
                    changed = True

            if st_pid:
                wm = distance_matrix_walk_minutes(lat, lng, st_pid)
                if wm is not None:
                    if OVERWRITE_WALK_MINUTES or safe_str(row.get("walk_minutes")).strip() == "" or should_fix_station(row):
                        row["walk_minutes"] = str(wm)
                        changed = True

    return changed


def main() -> None:
    if not API_KEY:
        raise RuntimeError("GOOGLE_MAPS_API_KEY が未設定です（GitHub Secrets を確認）")

    header, rows = load_csv_rows(MASTER_CSV)

    # 追加列（存在しなければ増やす）
    need_cols = [
        "facility_id", "name", "ward",
        "address", "lat", "lng", "map_url",
        "facility_type", "phone", "website", "notes",
        "nearest_station", "walk_minutes",
        "name_kana", "station_kana",
    ]
    for c in need_cols:
        if c not in header:
            header.append(c)

    # 修正対象の行を絞る
    target_idxs: List[int] = []
    for i, r in enumerate(rows):
        ensure_cols(r, need_cols)

        if WARD_FILTER and safe_str(r.get("ward")).strip() != WARD_FILTER:
            continue

        if not ONLY_BAD_ROWS:
            target_idxs.append(i)
            continue

        # bad rows only
        if should_fix_core(r) or should_fix_station(r) or safe_str(r.get("name_kana")).strip() == "" or safe_str(r.get("station_kana")).strip() == "":
            target_idxs.append(i)

    print(f"rows total={len(rows)} target={len(target_idxs)} ward={WARD_FILTER} only_bad={ONLY_BAD_ROWS} max_updates={MAX_UPDATES}")

    misses: List[Dict[str, str]] = []
    updated = 0
    updated_cells = 0

    for idx in target_idxs:
        if updated >= MAX_UPDATES:
            break
        before = dict(rows[idx])
        changed = fix_one_row(rows[idx], misses)
        if changed:
            updated += 1
            # rough count: how many fields changed
            for k in header:
                if safe_str(before.get(k)) != safe_str(rows[idx].get(k)):
                    updated_cells += 1

    write_csv_rows(MASTER_CSV, header, rows)

    # misses csv
    if misses:
        miss_header = ["facility_id", "name", "ward", "reason", "query"]
        with MISSES_CSV.open("w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=miss_header)
            w.writeheader()
            for m in misses:
                w.writerow({k: m.get(k, "") for k in miss_header})
        print(f"misses: {len(misses)} wrote {MISSES_CSV}")

    print(f"DONE. updated_rows={updated} updated_cells={updated_cells} wrote={MASTER_CSV}")


if __name__ == "__main__":
    main()
