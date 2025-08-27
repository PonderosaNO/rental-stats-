import os, re, csv, json, time, pathlib, datetime, urllib.parse, yaml
import requests
from bs4 import BeautifulSoup

ROOT = pathlib.Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
RAW  = DATA / "raw_html"
SNAP = DATA / "snapshots"
HIST = DATA / "history"
for p in [RAW, SNAP, HIST]: p.mkdir(parents=True, exist_ok=True)

# --- Regex som fanger opp både nye og gamle varianter av FINN-lenker ---
RE_AD_HREF = re.compile(
    r"(https?://)?(www\.)?finn\.no/realestate/lettings/(?:ad\.html\?finnkode=\d+|.*?/\d{6,})(?:[?#].*)?$",
    re.I
)
RE_FINNKODE = re.compile(r"(?:finnkode=|/)(\d{6,})")

# Tallmønstre
NUM_RE = r"(?:\d{1,3}(?:[ .]\d{3})+|\d+)"
RE_PRICE = re.compile(rf"({NUM_RE})\s*kr", re.I)
RE_SQM   = re.compile(rf"({NUM_RE})\s*(?:m2|m²|kvm)", re.I)
RE_BED   = re.compile(r"(\d+)\s*(?:soverom|sov|rom\b)", re.I)

def to_int(s): 
    try: return int(re.sub(r"[ .]", "", s))
    except: return None

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

def normalize(s):
    return " ".join((s or "").strip().replace(",", " ").split()).lower()

def extract_all_ad_urls_from_search_html(html):
    soup = BeautifulSoup(html, "html.parser")
    urls = set()
    # 1) Alle href-attributter – enkel og robust metode
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if href.startswith("/"):
            href = "https://www.finn.no" + href
        if RE_AD_HREF.search(href):
            urls.add(href.split("#")[0])
    # 2) Fallback: se etter tekst som ligner URL i hele HTML hvis nødvendig
    if not urls:
        for m in RE_AD_HREF.finditer(html):
            h = m.group(0)
            if not h.startswith("http"):
                h = "https://" + h
            urls.add(h.split("#")[0])
    return sorted(urls)

def parse_price_sqm_bed(text):
    price = sqm = beds = None
    m = RE_PRICE.search(text);  price = to_int(m.group(1)) if m else None
    m = RE_SQM.search(text);    sqm   = to_int(m.group(1)) if m else None
    for pat in [re.compile(r"(\d+)\s*soverom",re.I),re.compile(r"(\d+)\s*sov",re.I),re.compile(r"(\d+)\s*rom\b",re.I)]:
        m = pat.search(text)
        if m: 
            try: beds = int(m.group(1)); break
            except: pass
    return price, sqm, beds

def finn_id_from_any(url, html=None):
    # 1) Fra URL
    m = RE_FINNKODE.search(url)
    if m: return m.group(1)
    # 2) Prøv meta/og:url
    if html:
        soup = BeautifulSoup(html, "html.parser")
        og = soup.find("meta", attrs={"property":"og:url"})
        if og and og.get("content"):
            m2 = RE_FINNKODE.search(og["content"])
            if m2: return m2.group(1)
    return ""

def parse_detail(html):
    soup = BeautifulSoup(html, "html.parser")
    text_all = soup.get_text(" ", strip=True)

    # Tittel
    title = ""
    h = soup.find(["h1","h2"])
    if h: title = h.get_text(strip=True)

    # JSON-LD først
    address = city = postal = ""
    for s in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(s.string) if s.string else None
        except: 
            continue
        items = data if isinstance(data, list) else [data] if data else []
        for it in items:
            if isinstance(it, dict) and isinstance(it.get("address"), dict):
                a = it["address"]
                address = a.get("streetAddress") or address
                city    = a.get("addressLocality") or a.get("addressRegion") or city
                postal  = a.get("postalCode") or postal
        if address: break

    # Fallback: etiketter
    if not address:
        label = soup.find(string=re.compile(r"^Adresse:?$", re.I))
        if label:
            try:
                val = label.find_parent().find_next().get_text(" ", strip=True)
                if val: address = val
            except: pass

    price, sqm, beds = parse_price_sqm_bed(text_all)
    return {
        "title": title,
        "price_nok": price if price is not None else "",
        "sqm": sqm if sqm is not None else "",
        "bedrooms": beds if beds is not None else "",
        "address": address,
        "postal_code": postal,
        "city": city,
    }

def run_market(market, gcfg):
    ua = gcfg["user_agent"]
    max_pages = int(gcfg["max_pages"])
    page_sleep = float(gcfg["page_sleep_sec"])
    ad_sleep = float(gcfg["ad_sleep_sec"])

    base = market["search_url"]
    key  = market["key"]
    today = datetime.date.today().isoformat()

    # --- 1) Hent alle annonse-URLer fra søket ---
    ad_urls = []
    for page in range(1, max_pages+1):
        url = build_page_url(base, page)
        html = fetch(url, ua)
        (RAW / f"{key}_search_{page}.html").write_text(html, encoding="utf-8")

        urls = extract_all_ad_urls_from_search_html(html)
        # Heuristisk stopp hvis neste side ikke gir nye lenker
        new = [u for u in urls if u not in ad_urls]
        ad_urls.extend(new)
        print(f"[{key}] page {page}: fant {len(new)} nye annonser (totalt {len(ad_urls)})")

        # stopp hvis ingen nye eller færre enn 3 nye etter første side
        if page > 1 and len(new) == 0:
            break
        time.sleep(page_sleep)

    # Ingen annonser? Skriv tomt snapshot og returner
    fields = ["snapshot_date","market_key","source","country","city","currency","finn_id","title","url","price_nok","sqm","bedrooms","price_per_sqm","address","postal_code","address_key"]
    snap_fp = SNAP / f"{key}_{today}.csv"
    hist_fp = HIST / f"{key}.csv"

    if not ad_urls:
        with open(snap_fp, "w", newline="", encoding="utf-8") as f:
            csv.DictWriter(f, fieldnames=fields).writeheader()
        if not hist_fp.exists():
            with open(hist_fp, "w", newline="", encoding="utf-8") as f:
                csv.DictWriter(f, fieldnames=fields).writeheader()
        print(f"[{key}] Fant 0 annonser – skrev tomt snapshot.")
        return

    # --- 2) Besøk hver annonse og trekk ut detaljer ---
    rows = []
    for i,u in enumerate(ad_urls,1):
        try:
            html = fetch(u, ua)
        except Exception as e:
            print(f"[{key}] Feil ved henting av {u}: {e}")
            continue
        fid = finn_id_from_any(u, html) or str(i)
        (RAW / f"{key}_ad_{fid}.html").write_text(html, encoding="utf-8")
        d = parse_detail(html)
        ppk = ""
        if d["price_nok"] and d["sqm"]:
            try:
                ppk = round(int(d["price_nok"])/int(d["sqm"]), 2)
            except:
                ppk = ""
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

    # --- 3) Skriv snapshot + append historikk ---
    with open(snap_fp, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields); w.writeheader(); w.writerows(rows)

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
