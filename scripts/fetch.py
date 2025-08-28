import os, re, csv, json, time, pathlib, datetime, urllib.parse, yaml
import requests
from bs4 import BeautifulSoup

ROOT = pathlib.Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
RAW  = DATA / "raw_html"
SNAP = DATA / "snapshots"
HIST = DATA / "history"
for p in [RAW, SNAP, HIST]:
    p.mkdir(parents=True, exist_ok=True)

RE_AD_HREF = re.compile(r"(https?://)?(www\.)?finn\.no/realestate/lettings/.*?(\d{6,})(?:[?#].*)?$", re.I)
RE_FINNKODE = re.compile(r"(?:finnkode=|/)(\d{6,})")
NUM_RE = r"(?:\d{1,3}(?:[ .]\d{3})+|\d+)"
RE_PRICE_TXT = re.compile(rf"({NUM_RE})\s*kr", re.I)
RE_SQM       = re.compile(rf"({NUM_RE})\s*(?:m2|m²|kvm)", re.I)
RE_BED_TXT   = re.compile(r"\b(\d+)\s*(?:soverom|sov)\b", re.I)

def to_int(s):
    try: return int(re.sub(r"[ .]", "", str(s)))
    except: return None

def monthly_from_text(amount, context):
    if amount is None: return None
    ctx = (context or "").lower()
    if any(w in ctx for w in ["mnd","måned","monthly","per mnd","pr mnd"]): return amount
    if any(w in ctx for w in ["uke","weekly"]): return int(round(amount * 4.35))
    if any(w in ctx for w in ["dag","natt","daily","night"]): return int(round(amount * 30))
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
        if href.startswith("/"): href = "https://www.finn.no" + href
        if RE_AD_HREF.search(href): urls.add(href.split("#")[0])
    return sorted(urls)

def finn_id_from_any(url, html=None):
    m = RE_FINNKODE.search(url)
    if m: return m.group(1)
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

    # --- Tittel
    title = soup.find("meta", {"property":"og:title"})
    if title and title.get("content"): title = title["content"].strip()
    else:
        h = soup.find(["h1","h2"])
        title = h.get_text(strip=True) if h else ""

    # --- Pris
    price = None; ctx = ""
    for sel in [
        ('meta', {'property': 'product:price:amount'}, 'content'),
        ('meta', {'itemprop': 'price'}, 'content'),
    ]:
        tag = soup.find(sel[0], attrs=sel[1])
        if tag and tag.get(sel[2]): price = to_int(tag[sel[2]]); break
    if price is None:
        for s in soup.find_all("script", type="application/ld+json"):
            try: data = json.loads(s.string)
            except: continue
            if isinstance(data, dict) and "offers" in data:
                offers = data["offers"]
                if isinstance(offers, dict):
                    p = offers.get("price") or (offers.get("priceSpecification") or {}).get("price")
                    if p: price = to_int(p); ctx = json.dumps(offers).lower()
    if price is None:
        m = RE_PRICE_TXT.findall(text_all)
        if m:
            candidates = [to_int(x) for x in m if to_int(x) and to_int(x) > 3000]
            if candidates: price = max(candidates)
    price_mo = monthly_from_text(price, ctx or text_all)

    # --- Kvm
    sqm = None
    m = RE_SQM.search(text_all)
    if m: sqm = to_int(m.group(1))

    # --- Soverom
    bedrooms = None
    m = RE_BED_TXT.search(text_all)
    if m: bedrooms = int(m.group(1))

    # --- Adresse
    address = city = postal = area = ""
    for s in soup.find_all("script", type="application/ld+json"):
        try: data = json.loads(s.string)
        except: continue
        if isinstance(data, dict) and "address" in data:
            a = data["address"]
            address = a.get("streetAddress","") or address
            city = a.get("addressLocality","") or a.get("addressRegion","") or city
            postal = a.get("postalCode","") or postal
            area = a.get("addressRegion","") or area
    return {
        "title": title, "price_nok": price_mo or "",
        "sqm": sqm or "", "bedrooms": bedrooms or "",
        "address": address, "postal_code": postal, "city": city, "area": area
    }

def run_market(market, gcfg):
    ua = gcfg["user_agent"]
    today = datetime.date.today().isoformat()
    key = market["key"]

    urls = extract_all_ad_urls_from_search_html(fetch(build_page_url(market["search_url"],1), ua))
    rows=[]
    for u in urls:
        html = fetch(u, ua)
        fid = finn_id_from_any(u, html)
        d = parse_detail(html)
        if not d["price_nok"]: continue
        # Outlier-filter
        if not (2000 < int(d["price_nok"]) < 60000): continue
        if d["sqm"] and (int(d["sqm"])<10 or int(d["sqm"])>300): continue
        ppk = ""
        if d["sqm"]: ppk = round(int(d["price_nok"])/int(d["sqm"]))
        addr_key = "|".join(x.lower() for x in [d["address"], d["postal_code"], d["city"]] if x)
        rows.append({
            "snapshot_date": today,"market_key": key,"finn_id": fid,
            "title": d["title"],"url": u,
            "price_nok": d["price_nok"],"sqm": d["sqm"],"bedrooms": d["bedrooms"],"price_per_sqm": ppk,
            "address": d["address"],"postal_code": d["postal_code"],"city": d["city"],"area": d["area"],
            "address_key": addr_key
        })
    snap_fp = SNAP / f"{key}_{today}.csv"
    with open(snap_fp,"w",newline="",encoding="utf-8") as f:
        w=csv.DictWriter(f,fieldnames=rows[0].keys()); w.writeheader(); w.writerows(rows)
    print(f"[{key}] {len(rows)} annonser lagret")

def main():
    cfg=yaml.safe_load((ROOT/"config.yaml").read_text())
    for m in cfg["markets"]: run_market(m, cfg["global"])
if __name__=="__main__": main()
