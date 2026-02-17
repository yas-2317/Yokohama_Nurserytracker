// common.js — shared helpers for index.html / facility.html

function safeStr(x){
  return (x === null || x === undefined) ? "" : String(x);
}

function fmt(x){
  if (x === null || x === undefined) return "—";
  const s = String(x).trim();
  if (s === "" || s.toLowerCase() === "null" || s === "-") return "—";
  const n = Number(s);
  if (!Number.isFinite(n)) return "—";
  return n.toLocaleString("ja-JP");
}

// alias (people sometimes call fmtNum)
function fmtNum(x){
  return fmt(x);
}

function toIntOrNull(x){
  if (x === null || x === undefined) return null;
  const s = String(x).trim();
  if (s === "" || s.toLowerCase() === "null" || s === "-") return null;
  const n = Number(s);
  if (!Number.isFinite(n)) return null;
  return Math.trunc(n);
}

function getParam(key){
  try{
    const u = new URL(window.location.href);
    const v = u.searchParams.get(key);
    return v;
  }catch(e){
    // very old fallback
    const m = new RegExp("[?&]"+key+"=([^&]+)").exec(window.location.search);
    return m ? decodeURIComponent(m[1]) : null;
  }
}

// JSON loader with no-cache hint (GitHub Pages sometimes caches aggressively)
async function loadJSON(path){
  const url = path + (path.includes("?") ? "&" : "?") + "v=" + Date.now();
  const res = await fetch(url, { cache: "no-store" });
  if(!res.ok) throw new Error(`Failed to load ${path} (${res.status})`);
  return await res.json();
}

// Expose for safety (in case some scripts call window.xxx)
window.safeStr = safeStr;
window.fmt = fmt;
window.fmtNum = fmtNum;
window.toIntOrNull = toIntOrNull;
window.getParam = getParam;
window.loadJSON = loadJSON;
