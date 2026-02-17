#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
from pathlib import Path
from collections import Counter

DATA_DIR = Path("data")

def main():
    months = json.loads((DATA_DIR/"months.json").read_text(encoding="utf-8")).get("months", [])
    months = sorted(months)
    for m in months:
        p = DATA_DIR / f"{m}.json"
        if not p.exists():
            print(f"[{m}] MISSING FILE")
            continue
        obj = json.loads(p.read_text(encoding="utf-8"))
        facs = obj.get("facilities", [])
        wards = [str(f.get("ward","")).strip() for f in facs if isinstance(f, dict)]
        c = Counter([w for w in wards if w])
        top = ", ".join([f"{k}:{v}" for k,v in c.most_common(5)])
        print(f"[{m}] facilities={len(facs)} wards={len(c)} top={top}")

if __name__ == "__main__":
    main()
