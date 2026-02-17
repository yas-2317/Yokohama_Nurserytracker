// common.js
// Shared helpers for Yokohama Nursery Availability

function safeStr(x){
  return (x==null) ? "" : String(x);
}

function fmtNum(n){
  const x = Number(n);
  if(n==null || n==="" || !isFinite(x)) return "—";
  return x.toLocaleString('ja-JP');
}

// Backward compat: some pages use fmt()
function fmt(n){
  return fmtNum(n);
}

async function loadJSON(path){
  const res = await fetch(path, { cache: "no-store" });
  if(!res.ok){
    throw new Error(`Failed to load ${path} (${res.status})`);
  }
  return await res.json();
}

// Normalize Japanese search text a bit (lightweight)
function normalizeForSearch(s){
  // unify spaces + lowercase
  let t = safeStr(s).replace(/　/g, " ");
  t = t.replace(/\s+/g, " ").trim().toLowerCase();

  // katakana -> hiragana
  t = t.replace(/[\u30a1-\u30f6]/g, ch =>
    String.fromCharCode(ch.charCodeAt(0) - 0x60)
  );
  return t;
}
