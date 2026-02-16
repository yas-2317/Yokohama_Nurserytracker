#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import csv
import os
import re
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
MASTER_CSV = DATA_DIR / "master_facilities.csv"

API_KEY = (os.getenv("GOOGLE_MAPS_API_KEY") or "").strip()
if not API_KEY:
    raise SystemExit("ERROR: GOOGLE_MAPS_API_KEY is required")

# 叩きすぎ防止
SLEEP_SEC = float(os.getenv("GOOGLE_API_SLEEP_SEC", "0.1"))

LANG = "ja"
REGION = "jp"

# Places API (Legacy endpoints; stable & simple)
FIND_PLACE_URL = "https://maps.googleapis.com/maps/api/place/findplacefromtext/json"
DETAILS_URL = "https://maps.googleapis.com/maps/api/place/details/json"
TEXT_SEARCH_URL = "https://maps.googleapis.com/maps/api/place/textsearch/json"

# ---------- helpers ----------
def norm(s: str) -> str:
    s = (s or "").strip()
    s = s.replace("　", " ")
    s = re.sub(r"\s+", " ", s)
    return s.strip()

def norm_key(s: str) -> str:
    s = norm(s)
    s = s.lower()
    # 施設名の揺れを軽く吸収
    s = s.replace("（", "(").replace("）", ")")
    s = re.sub(r"[()\[\]「」『』・,，\.。、】【]", "", s)
    s = s.replace("保育園", "").replace("こども園", "").replace("認定こども園", "")
    s = s.replace("横浜", "").replace("市", "").replace("区", "")
    s = re.sub(r"\s+", "", s)
    return s

def looks_bad_address(address: str, ward: str) -> bool:
    a = norm(address)
    w = norm(ward)
    if a == "":
        return True
    # 最低限の整合：横浜市 + 区（港北区など）
    if "横浜市" not in a:
        return True
    if w and w not in a:
        return True
    return False

def safe_get(d: dict, path: List[str], default=None):
    cur = d
    for k in path:
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur

def request_json(url: str, params: dict, timeout: int = 30) -> dict:
    r = requests.get(url, params=params, timeout=timeout)
    r.raise_for_status()
    return r.json()

def find_place(query: str) -> List[dict]:
    params = {
        "input": query,
        "inputtype": "textquery",
        "fields": "place_id,name,formatted_address,geometry",
        "language": LANG,
        "region": REGION,
        "key": API_KEY,
    }
    js = request_json(FIND_PLACE_URL, params=params, timeout=30)
    status = js.get("status")
    if status != "OK":
        return []
    return js.get("candidates", []) or []

def text_search(query: str) -> List[dict]:
    params = {
        "query": query,
        "language": LANG,
        "region": REGION,
        "key": API_KEY,
    }
    js = request_json(TEXT_SEARCH_URL, params=params, timeout=30)
    status = js.get("status")
    if status != "OK":
        return []
    return js.get("results", []) or []

def place_details(place_id: str) -> Optional[dict]:
    # 欲しい項目だけ fields で指定（課金/レスポンス節約）
    # フィールド概念は Places の Data Fields に沿う  :contentReference[oaicite:6]{index=6}
    params = {
        "place_id": place_id,
        "fields": "place_id,name,formatted_address,geometry/location,formatted_phone_number,website,url",
        "language": LANG,
        "region": REGION,
        "key": API_KEY,
    }
    js = request_json(DETAILS_URL, params=params, timeout=30)
    status = js.get("status")
    if status != "OK":
        return None
    return js.get("result")

def choose_best(name: str, candidates: List[dict]) -> Optional[dict]:
    if not candidates:
        return None
    target = norm_key(name)
    best = None
    best_score = -1
    for c in candidates[:5]:
        cn = norm_key(str(c.get("name", "")))
        # 簡易スコア：共通部分の長さ
        score = 0
        if target and cn:
            # 部分一致を加点
            if target in cn or cn in target:
                score += 50
            # 共通文字数
            common = len(set(target) & set(cn))
            score += common
        # 住所が入ってれば少し加点
        if c.get("formatted_address"):
            score += 3
        if score > best_score:
            best, best_score = c, score
    return best

def read_master(path: Path) -> Tuple[List[str], List[Dict[str, str]]]:
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        r = csv.DictReader(f)
        fieldnames = r.fieldnames or []
        rows = [dict(x) for x in r]
    return fieldnames, rows

def write_master(path: Path, fieldnames: List[str], rows: List[Dict[str, str]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for row in rows:
            w.writerow(row)

def ensure_columns(fieldnames: List[str], required: List[str]) -> List[str]:
    s = set(fieldnames)
    for c in required:
        if c not in s:
            fieldnames.append(c)
            s.add(c)
    return fieldnames

# ---------- main ----------
def main():
    if not MASTER_CSV.exists():
        raise SystemExit(f"ERROR: not found: {MASTER_CSV}")

    fieldnames, rows = read_master(MASTER_CSV)

    # 必要カラム（あなたの定義に合わせる）
    required_cols = [
        "facility_id", "name", "ward",
        "address", "lat", "lng",
        "facility_type", "phone", "website", "notes",
        "nearest_station", "walk_minutes",
        "map_url",
    ]
    fieldnames = ensure_columns(fieldnames, required_cols)

    misses_path = DATA_DIR / "geocode_misses.csv"
    miss_rows = []

    updated = 0
    checked = 0

    for row in rows:
        fid = norm(row.get("facility_id", ""))
        name = norm(row.get("name", ""))
        ward = norm(row.get("ward", "")) or (WARD_FILTER or "")
        address = norm(row.get("address", ""))
        lat = norm(row.get("lat", ""))
        lng = norm(row.get("lng", ""))

        if not name:
            continue

        # 「要修正」判定：住所が怪しい or lat/lng空
        need_fix = looks_bad_address(address, ward) or (lat == "" or lng == "")
        if not need_fix:
            continue

        checked += 1

        # クエリを組み立て（保育園ワードを入れるとヒット率が上がりやすい）
        q = f"{name} {ward} 横浜市 保育園"
        q = q.strip()

        # 1) Find Place
        cand = find_place(q)
        best = choose_best(name, cand)

        # 2) 保険：Text Search
        if best is None:
            cand2 = text_search(q)
            best = choose_best(name, cand2)

        if best is None:
            miss_rows.append({"facility_id": fid, "name": name, "ward": ward, "query_tried": q, "reason": "no_candidates"})
            time.sleep(SLEEP_SEC)
            continue

        pid = best.get("place_id")
        if not pid:
            miss_rows.append({"facility_id": fid, "name": name, "ward": ward, "query_tried": q, "reason": "no_place_id"})
            time.sleep(SLEEP_SEC)
            continue

        det = place_details(pid)
        if not det:
            miss_rows.append({"facility_id": fid, "name": name, "ward": ward, "query_tried": q, "reason": "details_failed"})
            time.sleep(SLEEP_SEC)
            continue

        new_addr = norm(det.get("formatted_address", ""))
        loc_lat = safe_get(det, ["geometry", "location", "lat"], None)
        loc_lng = safe_get(det, ["geometry", "location", "lng"], None)
        new_phone = norm(det.get("formatted_phone_number", ""))
        new_web = norm(det.get("website", ""))
        new_url = norm(det.get("url", ""))

        # 最低限の品質チェック：横浜市＋区が入ってないなら更新しない（誤爆防止）
        if ward and (("横浜市" not in new_addr) or (ward not in new_addr)):
            miss_rows.append({"facility_id": fid, "name": name, "ward": ward, "query_tried": q, "reason": f"addr_mismatch:{new_addr}"})
            time.sleep(SLEEP_SEC)
            continue

        # 更新（空欄のみ埋めたいなら条件を変えてOK）
        row["address"] = new_addr or row.get("address", "")
        if loc_lat is not None:
            row["lat"] = str(loc_lat)
        if loc_lng is not None:
            row["lng"] = str(loc_lng)

        # phone/website/map_url は「空なら埋める」運用が安全
        if not norm(row.get("phone", "")) and new_phone:
            row["phone"] = new_phone
        if not norm(row.get("website", "")) and new_web:
            row["website"] = new_web
        if not norm(row.get("map_url", "")) and new_url:
            row["map_url"] = new_url

        updated += 1
        time.sleep(SLEEP_SEC)

    # 書き出し
    write_master(MASTER_CSV, fieldnames, rows)

    if miss_rows:
        with misses_path.open("w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=["facility_id", "name", "ward", "query_tried", "reason"])
            w.writeheader()
            for mr in miss_rows:
                w.writerow(mr)

    print("DONE.")
    print("total rows:", len(rows))
    print("checked (need_fix):", checked)
    print("updated:", updated)
    print("misses:", len(miss_rows), f"(see {misses_path.name})" if miss_rows else "")


if __name__ == "__main__":
    main()
