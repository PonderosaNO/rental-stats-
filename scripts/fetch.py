import os, re, csv, json, time, pathlib, datetime, urllib.parse, yaml
import requests
from bs4 import BeautifulSoup
from dateutil.parser import isoparse

ROOT = pathlib.Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
RAW  = DATA / "raw_html"
SNAP = DATA / "snapshots"
HIST = DATA / "history"
for p in [RAW, SNAP, HIST]: p.mkdir(parents=True, exist_ok=True)

num_re = r"(?:\d{1,3}(?:[ .]\d{3})+|\d+)"
re_price = re.compile(rf"({num_re})\s*kr", re.I)
re_sqm   = re.compile(rf"({num_re})\s*(?:m2|m²|kvm)", re.I)
re_bed   = re.compile(r"(\d+)\s*(?:soverom|sov|rom\b)", re.I)
re_finn_id = re.compile(r"/(\d{6,})")

def to_int(s): return int(re.sub(r"[ .]", "", s))

def fetch(url, ua):
    r = requests.get(url, headers={"User-Agent": ua}, timeout=30)
    r.raise_for_status()
    return r.text

def build_page_url(base, page):
    parsed = urllib.parse.urlparse(base)
    qs = urllib.parse.parse_qs(parsed.query)
    qs["page"] = [str(page)]
    new_qs = urllib.parse.urlencode(qs, doseq=True)
    return urllib.parse.urlunparse(parsed._replace(query=new_qs))

def find_list_cards(soup):
    cards = []
    for sel in ["article","div.ads__unit","div.result-item","div.search-result"]:
        cards.extend(soup.select(sel))
    if not cards:
        cards = soup.find_all("a", href=True)
    return cards

def extract_list_url(card):
    a = card.find("a", href=True)
    if not a: return None
    href = a["href"]
    if href.startswith("/"): href = "https://www.finn.no"+href
    if "finn.no" not in href: return None
    if "/realestate/lettings/" in href: return href
    return None

def parse_price_sqm_bed(text):
    price = sqm = beds = None
    m = re_price.search(text);  price = to_int(m.group(1)) if m else None
    m = re_sqm.search(text);    sqm   = to_int(m.group(1)) if m else None
    for pat in [re.compile(r"(\d+)\s*soverom",re.I),re.compile(r"(\d+)\s*sov",re.I),re.compile(r"(\d+)\s*rom\b",re.I)]:
        m = pat.search(text)
        if m: beds = int(m.group(1)); break
    return price, sqm, beds

def parse_detail(html):
    soup = BeautifulSoup(html, "html.parser")
    text_all = soup.get_text(" ", strip=True)

    # JSON-LD
    address, city, postal = "", "", ""
    for s in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(s.string) if s.string else None
        except: continue
        items = data if isinstance(data, list) else [data] if data else []
        for it in items:
            if isinstance(it, dict) and isinstance(it.get("address"), dict):
                addr = it["address"]
                address = addr.get("streetAddress") or address
                city    = addr.get("addressLocality") or addr.get("addressRegion") or city
                postal  = addr.get("postalCode") or postal
        if address: break

    # fallback etiketter
    if not address:
        label = soup.find(string=re.compile(r"^Adresse:?$", re.I))
        if label:
            val = label.find_parent().find_next().get_text(" ", strip=True)
            if val: address = val

    price, sqm, beds = parse_price_sqm_bed(text_all)
    title = (soup.find(["h1","h2"]).get_text(strip=True) if soup.find(["h1","h2"]) else "")
    return dict(title=title, price_nok=price or "", sqm=sqm or "", bedrooms=beds or "",
                address=address, postal_code=postal, city=city)

def finn_id(url):
    m = re_finn_id.search(url); return m.group(1) if m else ""

def normalize(s):
    return " ".join((s or "").strip().replace(",", " ").split()).lower()

def run_market(market, gcfg):
    ua = gcfg["user_agent"]
    max_pages = int(gcfg["max_pages"])
    page_sleep = float(gcfg["page_sleep_sec"])
    ad_sleep = float(gcfg["ad_sleep_sec"])

    base = market["search_url"]
    key  = market["key"]
    today = datetime.date.today().isoformat()

    # hent alle resultatsider
    ad_urls = []
    for page in range(1, max_pages+1):
        url = build_page_url(base, page)
        html = fetch(url, ua)
        (RAW / f"{key}_search_{page}.html").write_text(html, encoding="utf-8")
        soup = BeautifulSoup(html, "html.parser")
        if "Ingen resultater" in soup.get_text(" ", strip=True): break
        found = 0
        for card in find_list_cards(soup):
            u = extract_list_url(card)
            if u and u not in ad_urls:
                ad_urls.append(u); found += 1
        if found == 0 and page > 1: break
        time.sleep(page_sleep)

    rows = []
    for i,u in enumerate(ad_urls,1):
        try:
            html = fetch(u, ua)
        except Exception as e:
            print(f"Feil {u}: {e}"); continue
        fid = finn_id(u) or str(i)
        (RAW / f"{key}_ad_{fid}.html").write_text(html, encoding="utf-8")
        d = parse_detail(html)
        ppk = ""
        if d["price_nok"] and d["sqm"]:
            try: ppk = round(int(d["price_nok"])/int(d["sqm"]), 2)
            except: ppk = ""
        addr_key = " | ".join(x for x in [normalize(d["address"]), normalize(d["postal_code"]), normalize(d["city"])] if x)
        rows.append({
            "snapshot_date": today,
            "market_key": key,
            "source": market.get("source","FINN"),
            "country": market.get("country",""),
            "city": market.get("city",""),
            "currency": market.get("currency","NOK"),
            "finn_id": fid,
            "title": d["title"],
            "url": u,
            "price_nok": d["price_nok"],
            "sqm": d["sqm"],
            "bedrooms": d["bedrooms"],
            "price_per_sqm": ppk,
            "address": d["address"],
            "postal_code": d["postal_code"],
            "address_key": addr_key,
        })
        time.sleep(ad_sleep)

    # skriv snapshot og historikk
    fields = ["snapshot_date","market_key","source","country","city","currency","finn_id","title","url","price_nok","sqm","bedrooms","price_per_sqm","address","postal_code","address_key"]
    snap_fp = SNAP / f"{key}_{today}.csv"
    with open(snap_fp, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields); w.writeheader(); w.writerows(rows)

    hist_fp = HIST / f"{key}.csv"
    new_file = not hist_fp.exists()
    with open(hist_fp, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        if new_file: w.writeheader()
        w.writerows(rows)

    print(f"[{key}] lagret {len(rows)} rader -> {snap_fp} og append til {hist_fp}")

def main():
    cfg = yaml.safe_load((ROOT/"config.yaml").read_text(encoding="utf-8"))
    gcfg = cfg["global"]
    for m in cfg["markets"]:
        run_market(m, gcfg)

if __name__ == "__main__":
    main()
