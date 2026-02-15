async function loadJSON(path){
  const r = await fetch(path, {cache: "no-store"});
  if(!r.ok) throw new Error(`Failed to load ${path}`);
  return await r.json();
}

async function loadText(path){
  const r = await fetch(path, {cache:"no-store"});
  if(!r.ok) throw new Error(`Failed to load ${path}`);
  return await r.text();
}

function safeStr(x){ return (x==null) ? "" : String(x); }

function fmt(x){
  if(x==null || x==="") return "—";
  const n = Number(x);
  if(Number.isFinite(n)) return n.toLocaleString("ja-JP");
  return safeStr(x);
}

// very small csv parser (no quoted commas support; master_facilities.csv はシンプル前提)
function parseCSV(text){
  const lines = text.replace(/\r\n/g,"\n").replace(/\r/g,"\n").split("\n").filter(l=>l.trim()!=="");
  if(lines.length===0) return [];
  const header = lines[0].split(",").map(s=>s.trim());
  const rows = [];
  for(let i=1;i<lines.length;i++){
    const cols = lines[i].split(",");
    const obj = {};
    for(let j=0;j<header.length;j++){
      obj[header[j]] = (cols[j] ?? "").trim();
    }
    rows.push(obj);
  }
  return rows;
}

async function loadMaster(){
  try{
    const csv = await loadText("data/master_facilities.csv");
    const rows = parseCSV(csv);
    const mp = new Map();
    for(const r of rows){
      const id = safeStr(r.facility_id).trim();
      if(!id) continue;
      mp.set(id, r);
    }
    return mp;
  }catch(e){
    console.warn("master_facilities.csv not loaded:", e);
    return new Map();
  }
}

// monthJson.facilities に master をマージして返す
async function loadMonthFacilities(month){
  const [data, master] = await Promise.all([
    loadJSON(`data/${month}.json`),
    loadMaster()
  ]);

  const out = (data.facilities||[]).map(f=>{
    const id = safeStr(f.id);
    const m = master.get(id) || {};
    // ★ここで “表示用の最終値” を揃える（JSON側優先）
    return {
      ...f,
      address: safeStr(f.address || m.address || ""),
      nearest_station: safeStr(f.nearest_station || m.nearest_station || m.station || ""),
      walk_minutes: safeStr(f.walk_minutes || m.walk_minutes || ""),
      map_url: safeStr(f.map_url || m.map_url || ""),
      // 旧map_url列名互換
      mapUrl: safeStr(f.map_url || m.map_url || "")
    };
  });

  return {meta: data, facilities: out};
}

// URLパラメータ
function getParam(name){
  const u = new URL(location.href);
  return u.searchParams.get(name);
}
