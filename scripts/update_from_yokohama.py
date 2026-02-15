#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import csv
import json
import os
import re
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
from bs4 import BeautifulSoup

DATASET_PAGE = "https://data.city.yokohama.lg.jp/dataset/kodomo_nyusho-jokyo"

# envで上書き可（workflowのenvで設定）
WARD_FILTER = (os.getenv("WARD_FILTER", "港北区") or "").strip()
if WARD_FILTER == "":
    WARD_FILTER = None

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)


def norm(s: Any) -> str:
    """空白と全角空白を潰して比較しやすくする"""
    if s is None:
        return ""
    x = str(s).replace("　", " ")
    x = re.sub(r"\s+", "", x)
    return x.strip()


def to_int(x: Any) -> Optional[int]:
    """数値っぽいものをintへ。'-'は0扱い。"""
    if x is None:
        return None
    s = str(x).strip()
    if s == "" or s.lower() == "nan":
        return None
    if s in ("-", "－", "‐", "—", "―"):
        return 0
    try:
        return int(float(s))
    except Exception:
        return None


def detect_month(rows: List[Dict[str, str]]) -> str:
    """CSVに更新日があればそれを使い、なければ当月1日"""
    if rows:
        for k in ("更新日", "更新年月日", "更新日時", "更新年月"):
            v = str(rows[0].get(k, "")).strip()
            if v:
                return v[:10].replace("/", "-")
    today = date.today()
    return date(today.year, today.month, 1).isoformat()


def read_csv_from_url(url: str) -> List[Dict[str, str]]:
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    for enc in ("cp932", "shift_jis", "utf-8-sig", "utf-8"):
        try:
            text = r.content.decode(enc)
            break
        except UnicodeDecodeError:
            continue
    else:
        text = r.text
    return list(csv.DictReader(text.splitlines()))


def scrape_csv_urls() -> Dict[str, str]:
    """データセットページから accept/wait のCSV URLを推定して返す"""
    html = requests.get(DATASET_PAGE, timeout=30).text
    soup = BeautifulSoup(html, "html.parser")

    links = [a.get("href", "") for a in soup.select("a[href]") if a.get("href", "").endswith(".csv")]
    if not links:
        links = re.findall(r"https?://[^\s\"']+\.csv", html)
    links = list(dict.fromkeys(links))

    best: Dict[str, str] = {}

    # 既知ID（あるなら最優先）
    for url in links:
        if "0926_" in url:
            best["accept"] = url
        elif "0929_" in url:
            best["wait"] = url

    # キーワード推定（保険）
    if "accept" not in best:
        for url in links:
            if ("受入" in url) or ("入所可能" in url):
                best["accept"] = url
                break

    if "wait" not in best:
        for url in links:
            if "待ち" in url:
                best["wait"] = url
                break

    if "accept" not in best or "wait" not in best:
        raise RuntimeError("CSVリンク抽出に失敗（ページ仕様変更の可能性）")

    print("CSV URLs:", best)
    return best


def guess_key(row: Dict[str, str], candidates: List[str], contains: Optional[str] = None) -> Optional[str]:
    for k in candidates:
        if k in row:
            return k
    if contains:
        for k in row.keys():
            if contains in k:
                return k
    return None


def index_by_facility_id(rows: List[Dict[str, str]]) -> Dict[str, Dict[str, str]]:
    if not rows:
        raise RuntimeError("CSVが空です")

    fid_key = None
    for k in ("施設番号", "施設・事業所番号", "施設事業所番号", "事業所番号"):
        if k in rows[0]:
            fid_key = k
            break
    if not fid_key:
        for k in rows[0].keys():
            if "番号" in k:
                fid_key = k
                break
    if not fid_key:
        raise RuntimeError("施設番号列が見つかりません")

    out: Dict[str, Dict[str, str]] = {}
    for r in rows:
        fid = str(r.get(fid_key, "")).strip()
        if fid:
            out[fid] = r
    return out


def total_from(row: Dict[str, str]) -> Optional[int]:
    """合計列が揺れるので '合計' を含む列から拾う"""
    if not row:
        return None
    if "合計" in row and str(row.get("合計", "")).strip() != "":
        return to_int(row.get("合計"))
    for k in row.keys():
        if "合計" in k and str(row.get(k, "")).strip() != "":
            return to_int(row.get(k))
    return None


def main() -> None:
    print("START update_from_yokohama.py  WARD_FILTER=", WARD_FILTER)

    urls = scrape_csv_urls()
    accept_rows = read_csv_from_url(urls["accept"])
    wait_rows = read_csv_from_url(urls["wait"])

    month = detect_month(accept_rows)
    print("Detected month:", month)

    A = index_by_facility_id(accept_rows)
    W = index_by_facility_id(wait_rows)

    ward_key = guess_key(accept_rows[0], ["施設所在区", "所在区", "区名", "区"], contains="区")
    name_key = guess_key(accept_rows[0], ["施設・事業名", "施設名", "施設・事業所名", "事業名"], contains="施設")
    print("ward_key:", ward_key, "name_key:", name_key)

    target = norm(WARD_FILTER) if WARD_FILTER else None

    facilities: List[Dict[str, Any]] = []
    for fid, ar in A.items():
        ward = norm(ar.get(ward_key)) if ward_key else ""
        ward = ward.replace("横浜市", "")  # よくある接頭辞を除去

        if target and target not in ward:
            continue

        wr = W.get(fid, {})

        name = str(ar.get(name_key, "")).strip() if name_key else ""
        q = re.sub(r"\s+", " ", f"{name} {ward} 横浜市").strip()
        map_url = f"https://www.google.com/maps/search/?api=1&query={q}"

        tot_accept = total_from(ar)
        tot_wait = total_from(wr)

        facilities.append({
            "id": fid,
            "name": name,
            "ward": ward,
            "address": "",
            "map_url": map_url,
            "updated": month,
            "totals": {"accept": tot_accept, "wait": tot_wait},
            "ages": {}
        })

    print("facilities count:", len(facilities))
    if len(facilities) == 0:
        raise RuntimeError("facilitiesが0件です（区フィルタ/列名不一致の可能性）")

    # ✅ 月次JSONを必ず作る
    month_path = DATA_DIR / f"{month}.json"
    month_path.write_text(
        json.dumps({"month": month, "ward": (WARD_FILTER or "横浜市"), "facilities": facilities},
                   ensure_ascii=False, indent=2),
        encoding="utf-8"
    )
    if not month_path.exists() or month_path.stat().st_size < 200:
        raise RuntimeError("月次JSONが生成されていない/小さすぎます")

    # ✅ months.json を更新（壊れてても作り直す）
    months_path = DATA_DIR / "months.json"
    months: Dict[str, Any] = {"months": [month]}
    if months_path.exists():
        try:
            old_txt = months_path.read_text(encoding="utf-8").strip()
            old = json.loads(old_txt) if old_txt else {}
            ms = set(old.get("months", []))
            ms.add(month)
            months["months"] = sorted(ms)
        except Exception:
            months = {"months": [month]}

    months_path.write_text(json.dumps(months, ensure_ascii=False, indent=2), encoding="utf-8")

    print("WROTE:", month_path.name, "and months.json")


if __name__ == "__main__":
    main()
