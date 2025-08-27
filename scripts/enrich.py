import csv, pathlib, datetime, statistics, yaml

ROOT = pathlib.Path(__file__).resolve().parents[1]
HIST = ROOT/"data/history"
OUT  = ROOT/"data/by_address"
OUT.mkdir(parents=True, exist_ok=True)

def to_int(x): 
    try: return int(x)
    except: return None
def to_float(x):
    try: return float(x)
    except: return None
def to_date(x):
    try: return datetime.date.fromisoformat(x)
    except: return None

def process_market(market_key):
    hist_fp = HIST / f"{market_key}.csv"
    if not hist_fp.exists(): 
        print(f"[{market_key}] ingen historikk"); 
        return

    rows=[]
    with open(hist_fp, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            r["_date"]=to_date(r["snapshot_date"])
            r["_price"]=to_int(r["price_nok"])
            r["_ppk"]=to_float(r["price_per_sqm"])
            rows.append(r)

    # group by address_key
    by_addr={}
    for r in rows:
        k=r.get("address_key","")
        if not k: continue
        by_addr.setdefault(k,[]).append(r)

    # addresses_history
    hist_fields=["address_key","snapshot_date","finn_id","title","url","price_nok","sqm","bedrooms","price_per_sqm","address","postal_code","city","market_key","country","currency"]
    with open(OUT/f"{market_key}_addresses_history.csv","w",newline="",encoding="utf-8") as f:
        w=csv.DictWriter(f, fieldnames=hist_fields); w.writeheader()
        for k,lst in by_addr.items():
            for r in sorted(lst, key=lambda x: x["_date"] or datetime.date.min):
                w.writerow({
                    "address_key":k,"snapshot_date":r["snapshot_date"],"finn_id":r["finn_id"],"title":r["title"],"url":r["url"],
                    "price_nok":r["price_nok"],"sqm":r["sqm"],"bedrooms":r["bedrooms"],"price_per_sqm":r["price_per_sqm"],
                    "address":r["address"],"postal_code":r["postal_code"],"city":r["city"],
                    "market_key":r["market_key"],"country":r["country"],"currency":r["currency"]
                })

    # addresses_latest + trends
    latest_fields=["address_key","last_seen","observations","last_price","prev_price","price_change_abs","price_change_pct","avg_price","median_price","avg_price_per_sqm","median_price_per_sqm","address","postal_code","city","market_key"]
    with open(OUT/f"{market_key}_address_trends.csv","w",newline="",encoding="utf-8") as f:
        w=csv.DictWriter(f, fieldnames=latest_fields); w.writeheader()
        for k,lst in by_addr.items():
            lst_sorted=sorted(lst, key=lambda x: x["_date"] or datetime.date.min)
            prices=[r["_price"] for r in lst_sorted if r["_price"] is not None]
            ppk=[r["_ppk"] for r in lst_sorted if r["_ppk"] is not None]
            last=lst_sorted[-1]
            prev_price=prices[-2] if len(prices)>=2 else None
            last_price=prices[-1] if prices else None
            change_abs= "" if last_price is None or prev_price is None else last_price - prev_price
            change_pct= "" if not isinstance(change_abs,int) or prev_price==0 else round(100.0*change_abs/prev_price,2)
            w.writerow({
                "address_key":k,"last_seen":last["snapshot_date"],"observations":len(lst_sorted),
                "last_price": last_price if last_price is not None else "",
                "prev_price": prev_price if prev_price is not None else "",
                "price_change_abs": change_abs if change_abs!="" else "",
                "price_change_pct": change_pct if change_pct!="" else "",
                "avg_price": round(statistics.mean(prices),2) if prices else "",
                "median_price": statistics.median(prices) if prices else "",
                "avg_price_per_sqm": round(statistics.mean(ppk),2) if ppk else "",
                "median_price_per_sqm": statistics.median(ppk) if ppk else "",
                "address": last["address"], "postal_code": last["postal_code"], "city": last["city"],
                "market_key": last["market_key"]
            })
    print(f"[{market_key}] skrevet by_address filer")

def main():
    cfg = yaml.safe_load((ROOT/"config.yaml").read_text(encoding="utf-8"))
    for m in cfg["markets"]:
        process_market(m["key"])

if __name__=="__main__":
    main()
