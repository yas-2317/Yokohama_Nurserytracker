# 横浜市 保育園：受入可能数・入所待ち人数 トラッカー

横浜市が毎月公開する保育園データを自動取得・可視化する静的サイトです。
受入可能数・入所待ち人数を区・年齢別に検索・比較できます。

**データソース：** [横浜市オープンデータ「保育所等の入所状況（毎月1日時点）」](https://data.city.yokohama.lg.jp/dataset/kodomo_nyusho-jokyo)

---

## ローカルで確認する

```bash
# リポジトリをクローン
git clone https://github.com/yas-2317/Yokohama_Nurserytracker.git
cd Yokohama_Nurserytracker

# index.html をブラウザで開くだけで動作します（サーバー不要）
open index.html        # macOS
start index.html       # Windows
xdg-open index.html   # Linux
```

---

## ファイル構成

```
.
├── index.html                    # 一覧ページ（メイン）
├── facility.html                 # 施設詳細ページ
├── common.js                     # 共通ユーティリティ
├── styles.css                    # スタイル定義
├── requirements.txt              # Python 依存関係
├── .env.example                  # 環境変数テンプレート
│
├── data/
│   ├── YYYY-MM-01.json           # 月次データ（施設ごとの受入・待機数）
│   ├── months.json               # 利用可能な月リスト
│   ├── master_facilities.csv     # 施設マスター（住所・地図・電話等）
│   ├── geocode_cache.json        # ジオコードキャッシュ
│   └── stations_cache_yokohama.json  # 駅情報キャッシュ
│
├── scripts/
│   ├── update_from_yokohama.py        # 月次データ取得・JSON生成（メイン）
│   ├── apply_master_to_all_months.py  # 全月JSONにマスター情報を適用
│   ├── backfill_last_year.py          # 過去データの遡及取得
│   ├── fix_master_with_google_places.py  # Google Places APIで住所・駅情報を補完
│   └── audit_months.py                # データ整合性チェック
│
└── .github/workflows/
    ├── update.yml       # 毎週自動更新（月次データ）
    ├── backfill.yml     # 手動：過去データの遡及取得
    └── fix_master.yml   # 手動：Google Places APIでマスター補完
```

---

## 自動更新の仕組み

```
毎週月曜 02:20 JST（GitHub Actions）
        ↓
update_from_yokohama.py
  └─ 横浜市データセットページをスクレイプ
  └─ 受入可能数CSV・入所待ちCSV・入所児童数CSV を取得
  └─ 3CSVをマージ → data/YYYY-MM-01.json を生成
        ↓
apply_master_to_all_months.py
  └─ data/master_facilities.csv の情報（住所・地図URL・電話等）を
     全月JSONに上書き反映
        ↓
git commit & push → GitHub Pages に自動デプロイ
```

---

## スクリプトの手動実行

```bash
# Python 依存関係のインストール
pip install -r requirements.txt

# 環境変数の設定（.env.exampleをコピーして編集）
cp .env.example .env

# 今月のデータを手動取得
WARD_FILTER=港北区 python scripts/update_from_yokohama.py

# 全月JSONにマスターを適用
python scripts/apply_master_to_all_months.py

# 過去データの遡及取得（例：直近36ヶ月）
MONTHS_BACK=36 python scripts/backfill_last_year.py

# データ整合性チェック
python scripts/audit_months.py
```

---

## GitHub Secrets の設定

`fix_master_with_google_places.py` を GitHub Actions から実行するには Secrets が必要です。

| Secret 名 | 説明 |
|---|---|
| `GOOGLE_API_KEY` | Google Places API キー |

設定場所：リポジトリ → Settings → Secrets and variables → Actions

---

## 施設マスターの編集

住所・地図URL・電話・最寄り駅などを手動で追加・修正する場合：

**`data/master_facilities.csv`** を直接編集してください。

| 列名 | 説明 |
|---|---|
| `facility_id` | 施設ID（変更不可） |
| `name` | 施設名 |
| `address` | 住所 |
| `lat` / `lng` | 緯度・経度 |
| `map_url` | Google Maps URL |
| `phone` | 電話番号 |
| `website` | 公式サイト |
| `nearest_station` | 最寄り駅名 |
| `walk_minutes` | 徒歩分数 |
| `notes` | 備考（休止中・廃止等） |

編集後、`apply_master_to_all_months.py` を実行すると全月JSONに反映されます。

---

## データ構造（JSON スキーマ）

各月ファイル（例：`data/2026-02-01.json`）の構造：

```json
{
  "month": "2026-02-01",
  "ward": "横浜市",
  "facilities": [
    {
      "id": "1410051018778",
      "name": "横浜市馬場保育園",
      "name_kana": "よこはましばばほいくえん",
      "ward": "鶴見区",
      "address": "神奈川県横浜市鶴見区...",
      "lat": "35.507859",
      "lng": "139.651384",
      "map_url": "https://maps.google.com/?cid=...",
      "phone": "+81 45-573-0054",
      "website": "https://...",
      "nearest_station": "菊名駅",
      "walk_minutes": "23",
      "totals": {
        "accept": 3,
        "wait": 3,
        "enrolled": 64
      },
      "age_groups": {
        "0":   { "accept": 0, "wait": 0, "enrolled": 0 },
        "1":   { "accept": 0, "wait": 2, "enrolled": 6 },
        "2":   { "accept": 0, "wait": 1, "enrolled": 9 },
        "3-5": { "accept": 3, "wait": 0, "enrolled": 49 }
      }
    }
  ]
}
```

---

## トラブルシューティング

### 月次更新が失敗する
- 横浜市のデータセットページのURL・CSV形式が変わった可能性があります。
- `scripts/update_from_yokohama.py` の `DATASET_PAGE` 定数と CSV 解析ロジックを確認してください。

### apply_master が反映されない
- `data/master_facilities.csv` の `facility_id` が JSON 内の `id` と一致しているか確認してください。

### Google Places APIエラー
- `GOOGLE_API_KEY` が正しく設定されているか確認してください。
- API の有効化：[Google Cloud Console](https://console.cloud.google.com/apis/library/places-backend.googleapis.com)

### ローカルでJSONが読み込まれない
- ブラウザのセキュリティポリシーにより、`file://` プロトコルでは CORS エラーが出る場合があります。
- その場合は簡易サーバーを使ってください：
  ```bash
  python -m http.server 8000
  # ブラウザで http://localhost:8000 を開く
  ```

---

出典：横浜市「保育所等の入所状況（毎月1日時点）」公開データ
