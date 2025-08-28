import os, re, csv, json, time, pathlib, datetime, urllib.parse, yaml
import requests
from bs4 import BeautifulSoup

# ---------- Paths ----------
ROOT = pathlib.Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
RAW  = DATA / "raw_html"
SNAP = DATA / "snapshots"
HIST = DATA / "history"
for p in [RAW, SNAP, HIST]:
    p.mkdir(parents=True, exist_ok=True)

# ---------- Regexer ----------
RE_AD_HREF = re.compile(
    r"(https?://)?(www\.)?finn\.no/realestate/lettings/(?:ad\.html\?finnkode=\d+|.*?/\d{6,})(?:[?#].*)?$",
    re.I
)
RE_FINNKODE = re.compile(r"(?:finnkode=|/)(\d{6,})")
NUM_RE = r"(?:\d{1,3}(?:[ .]\d{3})+|\d+)"
RE_PRICE_TXT = re.compile(rf"({NUM_RE})\s*kr", re.I)
RE_SQM       = re.compile(rf"({NUM_RE})\s*(?:m2|m²|kvm)", re.I)
RE_BED_TXT   = re.compile(r"\b(\d+)\s*(?:soverom|sov)\b", re.I)

# ---------- Hjelpere ----------
def to_int(s):
    try:
        return int(re.sub(r"[ .]", "", str(s)))
    except:
        return None

def monthly_from_text(amount, context):
    if amount is None:
        return None
    ctx = (context or "").lower()
    if any(w in ctx for w in ["mnd", "måned", "monthly", "per mnd", "pr mnd"]):
        return amount
    if any(w in ctx for w in ["uke", "weekly", "per uke"]):
        return int(round(amount * 4.35))
    if any(w in ctx for w in ["dag", "natt", "daily", "night"]):
        return int(round(amount * 30))
    return amount

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

def extract_all_ad_urls_from_search_html(html):
    soup = BeautifulSoup(html, "html.parser")
    urls = set()
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if href.startswith("/"):
            href = "https://www.finn.no" + href
        if RE_AD_HREF.search(href):
            urls.add(href.split("#")[0])
    return sorted(urls)

def finn_id_from_any(url, html=None):
    m = RE_FINNKODE.search(url)
    if m:
        return m.group(1)
    if html:
        soup = BeautifulSoup(html, "html.parser")
        og = soup.find("meta", attrs={"property": "og:url"})
        if og and og.get("content"):
            m2 = RE_FINNKODE.search(og["content"])
            if m2:
                return m2.group(1)
    return ""

# ---------- Pris-henter ----------
def extract_price(soup, text_all):
    price = None
    ctx = ""

    # 1) Meta-tags
    for sel in [
        ('meta', {'property': 'product:price:amount'}, 'content'),
        ('meta', {'itemprop': 'price'}, 'content'),
        ('meta', {'property': 'og:price:amount'}, 'content'),
    ]:
        tag = soup.find(sel[0], attrs=sel[1])
        if tag and tag.get(sel[2]):
            price = to_int(tag.get(sel[2]))
            break

    # 2) JSON-LD
    if price is None:
        for s in soup.find_all("script", type="application/ld+json"):
            try:
                data = json.loads(s.string) if s.string else None
            except:
                continue
            items = data if isinstance(data, list) else [data] if data else []
            for it in items:
                if not isinstance(it, dict):
                    continue
                offers = it.get("offers") or {}
                if isinstance(offers, list) and offers:
                    offers = offers[0]
                if isinstance(offers, dict):
                    p = offers.get("price") or (offers.get("priceSpecification") or {}).get("price")
                    if p:
                        price = to_int(p)
                        ctx = json.dumps(offers).lower()
                        break
            if price:
                break

    # 3) Key/Value-liste (eks "Leie per måned")
    if price is None:
        for label in soup.find_all(["dt","th"]):
            if not label.get_text(strip=True):
                continue
            text = label.get_text(strip=True).lower()
            if any(k in text for k in ["leie", "totalpris", "månedsleie", "per måned"]):
                val = label.find_next(["dd","td"])
                if val:
                    m = RE_PRICE_TXT.search(val.get_text(" ", strip=True))
                    if m:
                        price = to_int(m.group(1))
                        ctx = val.get_text(" ", strip=True)
                        break

    # 4) Regex fallback
    if price is None:
        m = RE_PRICE_TXT.findall(text_all)
        if m:
            candidates = [to_int(x) for x in m if to_int(x) and to_int(x) > 3000]
            if candidates:
                price = max(candidates)

    return monthly_from_text(price, ctx or text_all)

# ---------- Parser ----------
def parse_detail(html):
    soup = BeautifulSoup(html, "html.parser")
    text_all = soup.get_text(" ", strip=True)

    # Tittel
    title = ""
    h = soup.find(["h1","h2"])
    if h: title = h.get_text(strip=True)
    ogt = soup.find("meta", attrs={"property": "og:title"})
    if ogt and ogt.get("content"):
        title = ogt["content"].strip()

    # Pris
    price_mo = extract_price(soup, text_all)

    # Kvm
    sqm = None
    m = RE_SQM.search(text_all)
    sqm = to_int(m.group(1)) if m else None

    # Soverom
    bedrooms = None
    m = RE_BED_TXT.search(text_all)
    if m:
        bedrooms = int(m.group(1))

    # Adresse
    address = city = postal = area = ""
    for s in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(s.string) if s.string else None
        except:
            continue
        if isinstance(data, dict) and "address" in data:
            a = data["address"]
            if isinstance(a, dict):
                address = a.get("streetAddress") or address
                city    = a.get("addressLocality") or a.get("addressRegion") or city
                postal  = a.get("postalCode") or postal
                area    = a.get("addressRegion") or area

    return {
        "title": title,
        "price_nok": price_mo if price_mo else "",
        "sqm": sqm or "",
        "bedrooms": bedrooms or "",
        "address": address,
        "postal_code": postal,
        "city": city,
        "area": area,
    }

# ---------- Hovedløp ----------
def run_market(market, gcfg):
    ua         = gcfg.get("user_agent", "RentalStatsBot/1.0")
    max_pages  = int(gcfg.get("max_pages", 40))
    page_sleep = float(gcfg.get("page_sleep_sec", 1.5))
    ad_sleep   = float(gcfg.get("ad_sleep_sec", 0.8))
    min_price  = int(gcfg.get("min_price_nok", 2000))
    max_price  = int(gcfg.get("max_price_nok", 100000))
    min_sqm    = int(gcfg.get("min_sqm", 10))
    max_sqm    = int(gcfg.get("max_sqm", 400))

    today = datetime.date.today().isoformat()
    key   = market["key"]
    base  = market["search_url"]

    # --- 1) Paginering ---
    ad_urls = []
    for page in range(1, max_pages+1):
        url = build_page_url(base, page)
        try:
            html = fetch(url, ua)
        except Exception as e:
            print(f"[{key}] feil på side {page}: {e}")
            break
        (RAW / f"{key}_search_{page}.html").write_text(html, encoding="utf-8")
        urls = extract_all_ad_urls_from_search_html(html)
        new = [u for u in urls if u not in ad_urls]
        ad_urls.extend(new)
        print(f"[{key}] page {page}: {len(new)} nye lenker (totalt {len(ad_urls)})")
        if page>1 and not new:
            break
        time.sleep(page_sleep)

    ad_urls = list(dict.fromkeys(ad_urls))
    print(f"[{key}] Totalt {len(ad_urls)} URLer")

    # --- 2) Parse annonser ---
    rows = []
    dropped = {"no_price":0,"price":0,"sqm":0}
    for i,u in enumerate(ad_urls,1):
        try:
            html = fetch(u, ua)
        except Exception as e:
            print(f"[{key}] fetch-feil {u}: {e}")
            continue
        fid = finn_id_from_any(u, html) or str(i)
        (RAW / f"{key}_ad_{fid}.html").write_text(html, encoding="utf-8")
        d = parse_detail(html)

        if not d["price_nok"]:
            dropped["no_price"] += 1
            continue
        price_val = int(d["price_nok"])
        if not (min_price < price_val < max_price):
            dropped["price"] += 1
            continue
        sqm_val = to_int(d["sqm"]) if d["sqm"] else None
        if sqm_val and (sqm_val<min_sqm or sqm_val>max_sqm):
            dropped["sqm"] += 1
            continue

        ppk = round(price_val/sqm_val) if sqm_val else ""
        addr_key = "|".join(x.lower() for x in [d["address"],d["postal_code"],d["city"]] if x)
        rows.append({
            "snapshot_date": today,
            "market_key": key,
            "finn_id": fid,
            "title": d["title"],
            "url": u,
            "price_nok": price_val,
            "sqm": d["sqm"] or "",
            "bedrooms": d["bedrooms"] or "",
            "price_per_sqm": ppk,
            "address": d["address"],
            "postal_code": d["postal_code"],
            "city": d["city"] or d.get("area",""),
            "area": d.get("area",""),
            "address_key": addr_key
        })
        time.sleep(ad_sleep)

    print(f"[{key}] beholdt {len(rows)} annonser. Droppet: {dropped}")

    # --- 3) Lagre snapshot + history ---
    fields=["snapshot_date","market_key","finn_id","title","url","price_nok","sqm","bedrooms","price_per_sqm","address","postal_code","city","area","address_key"]
    snap_fp = SNAP / f"{key}_{today}.csv"
    with open(snap_fp,"w",newline="",encoding="utf-8") as f:
        w=csv.DictWriter(f,fieldnames=fields); w.writeheader(); w.writerows(rows)
    hist_fp = HIST / f"{key}.csv"
    write_header = not hist_fp.exists()
    with open(hist_fp,"a",newline="",encoding="utf-8") as f:
        w=csv.DictWriter(f,fieldnames=fields)
        if write_header: w.writeheader()
        w.writerows(rows)
    print(f"[{key}] skrevet {snap_fp.name} og appendet {hist_fp.name}")

def main():
    cfg=yaml.safe_load((ROOT/"config.yaml").read_text(encoding="utf-8"))
    gcfg=cfg.get("global",{})
    for m in cfg["markets"]:
        run_market(m,gcfg)

if __name__=="__main__":
    main()
