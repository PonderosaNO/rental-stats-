import csv, pathlib, statistics, yaml

ROOT=pathlib.Path(__file__).resolve().parents[1]
SNAP=ROOT/"data/snapshots"
SUM =ROOT/"data/summaries"; SUM.mkdir(parents=True,exist_ok=True)

def to_int(x):
    try: return int(x)
    except: return None

def med(arr):
    return statistics.median(arr) if arr else ""

def q25(arr):
    return statistics.quantiles(arr, n=4)[0] if len(arr) >= 4 else ""

def q75(arr):
    return statistics.quantiles(arr, n=4)[2] if len(arr) >= 4 else ""

def summarize_market(market_key):
    files=sorted(SNAP.glob(f"{market_key}_*.csv"))
    if not files: return None, []
    latest=files[-1]
    rows=[r for r in csv.DictReader(open(latest,encoding="utf-8"))]

    prices=[to_int(r["price_nok"]) for r in rows if r.get("price_nok")]
    sqms=[to_int(r["sqm"]) for r in rows if r.get("sqm")]
    ppk=[to_int(r["price_per_sqm"]) for r in rows if r.get("price_per_sqm")]

    out={
      "market_key": market_key,
      "snapshot_file": latest.name,
      "snapshot_date": latest.stem.split("_")[-1],
      "listings": len(rows),
      "avg_price": round(statistics.mean([p for p in prices if p is not None])) if prices else "",
      "median_price": med([p for p in prices if p is not None]),
      "p25_price": q25([p for p in prices if p is not None]),
      "p75_price": q75([p for p in prices if p is not None]),
      "avg_sqm": round(statistics.mean([s for s in sqms if s is not None])) if sqms else "",
      "avg_price_per_sqm": round(statistics.mean([v for v in ppk if v is not None])) if ppk else "",
    }

    # per soverom
    by_bed={}
    for r in rows:
        b = r.get("bedrooms")
        try: b=int(b)
        except: continue
        by_bed.setdefault(b, []).append(r)

    bed_rows=[]
    for b, lst in sorted(by_bed.items()):
        bp=[to_int(x.get("price_nok")) for x in lst if x.get("price_nok")]
        bs=[to_int(x.get("sqm")) for x in lst if x.get("sqm")]
        bppk=[to_int(x.get("price_per_sqm")) for x in lst if x.get("price_per_sqm")]
        bed_rows.append({
            "market_key": market_key,
            "snapshot_date": out["snapshot_date"],
            "bedrooms": b,
            "listings": len(lst),
            "avg_price": round(statistics.mean([p for p in bp if p is not None])) if bp else "",
            "avg_sqm": round(statistics.mean([s for s in bs if s is not None])) if bs else "",
            "avg_price_per_sqm": round(statistics.mean([v for v in bppk if v is not None])) if bppk else "",
        })

    return out, bed_rows

def main():
    cfg=yaml.safe_load((ROOT/"config.yaml").read_text(encoding="utf-8"))
    markets=[m["key"] for m in cfg["markets"]]

    rows=[]; bed_all=[]
    for k in markets:
        o, b = summarize_market(k)
        if o: rows.append(o)
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
