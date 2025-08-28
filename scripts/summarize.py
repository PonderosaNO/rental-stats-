import csv, pathlib, statistics, yaml

ROOT=pathlib.Path(__file__).resolve().parents[1]
SNAP=ROOT/"data/snapshots"
SUM =ROOT/"data/summaries"; SUM.mkdir(parents=True,exist_ok=True)

def to_int(x):
    try: return int(x)
    except: return None

def summarize(market_key):
    files=sorted(SNAP.glob(f"{market_key}_*.csv"))
    if not files: return None,[],[]
    latest=files[-1]
    rows=[r for r in csv.DictReader(open(latest,encoding="utf-8"))]
    prices=[to_int(r["price_nok"]) for r in rows if r["price_nok"]]
    sqms=[to_int(r["sqm"]) for r in rows if r["sqm"]]
    ppk=[to_int(r["price_per_sqm"]) for r in rows if r["price_per_sqm"]]
    def med(x): return statistics.median(x) if x else ""
    def p25(x): return statistics.quantiles(x,n=4)[0] if len(x)>=4 else ""
    def p75(x): return statistics.quantiles(x,n=4)[2] if len(x)>=4 else ""
    out={
      "market_key": market_key,"snapshot_file": latest.name,"snapshot_date": latest.stem.split("_")[-1],
      "listings": len(rows),"avg_price": round(statistics.mean(prices)) if prices else "",
      "median_price": med(prices),"p25_price": p25(prices),"p75_price": p75(prices),
      "avg_sqm": round(statistics.mean(sqms)) if sqms else "",
      "avg_price_per_sqm": round(statistics.mean(ppk)) if ppk else ""
    }
    # by bedrooms
    by_bed={}; bed_out=[]
    for r in rows:
        b=r["bedrooms"]; 
        if not b: continue
        b=int(b); by_bed.setdefault(b,[]).append(r)
    for b,lst in by_bed.items():
        bp=[to_int(x["price_nok"]) for x in lst if x["price_nok"]]
        bed_out.append({"market_key":market_key,"snapshot_date":out["snapshot_date"],"bedrooms":b,
                        "listings":len(lst),"avg_price":round(statistics.mean(bp)) if bp else ""})
    return out, bed_out, rows

def main():
    cfg=yaml.safe_load((ROOT/"config.yaml").read_text())
    mk=[m["key"] for m in cfg["markets"]]
    outs=[]; beds=[]
    for k in mk:
        o,b,_=summarize(k); 
        if o: outs.append(o); beds+=b
    if outs:
        with open(SUM/"markets_summary.csv","w",newline="",encoding="utf-8") as f:
            w=csv.DictWriter(f,fieldnames=outs[0].keys()); w.writeheader(); w.writerows(outs)
    if beds:
        with open(SUM/"markets_by_bedrooms.csv","w",newline="",encoding="utf-8") as f:
            w=csv.DictWriter(f,fieldnames=beds[0].keys()); w.writeheader(); w.writerows(beds)
if __name__=="__main__": main()
