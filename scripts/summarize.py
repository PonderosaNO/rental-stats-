import csv, pathlib, statistics, yaml

ROOT=pathlib.Path(__file__).resolve().parents[1]
SNAP=ROOT/"data/snapshots"
SUM =ROOT/"data/summaries"
SUM.mkdir(parents=True, exist_ok=True)

def to_int(x):
    try: return int(x)
    except: return None
def to_float(x):
    try: return float(x)
    except: return None

def summarize_market(market_key):
    files=sorted(SNAP.glob(f"{market_key}_*.csv"))
    if not files: return None, []
    latest=files[-1]
    rows=[]
    with open(latest, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f): rows.append(r)

    prices=[to_int(r.get("price_nok")) for r in rows if r.get("price_nok")]
    sqms=[to_int(r.get("sqm")) for r in rows if r.get("sqm")]
    ppk=[to_float(r.get("price_per_sqm")) for r in rows if r.get("price_per_sqm")]

    out={
        "market_key": market_key,
        "snapshot_file": latest.name,
        "snapshot_date": latest.stem.split("_")[-1],
        "listings": len(rows),
        "avg_price": round(statistics.mean([p for p in prices if p is not None]),2) if prices else "",
        "avg_sqm": round(statistics.mean([s for s in sqms if s is not None]),2) if sqms else "",
        "avg_price_per_sqm": round(statistics.mean([v for v in ppk if v is not None]),2) if ppk else "",
    }

    # per antall soverom
    by_bed={}
    for r in rows:
        try: b=int(r.get("bedrooms") or "")
        except: continue
        by_bed.setdefault(b, []).append(r)

    bed_rows=[]
    for b, lst in sorted(by_bed.items()):
        b_prices=[to_int(x.get("price_nok")) for x in lst if x.get("price_nok")]
        b_sqms=[to_int(x.get("sqm")) for x in lst if x.get("sqm")]
        b_ppk=[to_float(x.get("price_per_sqm")) for x in lst if x.get("price_per_sqm")]
        bed_rows.append({
            "market_key": market_key,
            "snapshot_date": out["snapshot_date"],
            "bedrooms": b,
            "listings": len(lst),
            "avg_price": round(statistics.mean([p for p in b_prices if p is not None]),2) if b_prices else "",
            "avg_sqm": round(statistics.mean([s for s in b_sqms if s is not None]),2) if b_sqms else "",
            "avg_price_per_sqm": round(statistics.mean([v for v in b_ppk if v is not None]),2) if b_ppk else "",
        })

    return out, bed_rows

def main():
    cfg=yaml.safe_load((ROOT/"config.yaml").read_text(encoding="utf-8"))
    markets=[m["key"] for m in cfg["markets"]]
    rows=[]; bed_all=[]
    for k in markets:
        s, b = summarize_market(k)
        if s: rows.append(s)
        bed_all += b
    if rows:
        with open(SUM/"markets_summary.csv","w",newline="",encoding="utf-8") as f:
            w=csv.DictWriter(f, fieldnames=rows[0].keys()); w.writeheader(); w.writerows(rows)
    if bed_all:
        with open(SUM/"markets_by_bedrooms.csv","w",newline="",encoding="utf-8") as f:
            w=csv.DictWriter(f, fieldnames=bed_all[0].keys()); w.writeheader(); w.writerows(bed_all)
    print("Skrev summaries/markets_summary.csv og markets_by_bedrooms.csv")

if __name__=="__main__":
    main()
