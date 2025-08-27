import csv, pathlib, statistics, yaml, datetime
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
    # finn siste snapshot for markedet
    files=sorted(SNAP.glob(f"{market_key}_*.csv"))
    if not files: return None
    latest=files[-1]
    rows=[]
    with open(latest, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f): rows.append(r)
    prices=[to_int(r["price_nok"]) for r in rows if r["price_nok"]]
    sqms=[to_int(r["sqm"]) for r in rows if r["sqm"]]
    ppk=[to_float(r["price_per_sqm"]) for r in rows if r["price_per_sqm"]]
    out={
        "market_key": market_key,
        "snapshot_file": latest.name,
        "snapshot_date": latest.stem.split("_")[-1],
        "listings": len(rows),
        "avg_price": round(statistics.mean([p for p in prices if p is not None]),2) if prices else "",
        "avg_sqm": round(statistics.mean([s for s in sqms if s is not None]),2) if sqms else "",
        "avg_price_per_sqm": round(statistics.mean([v for v in ppk if v is not None]),2) if ppk else "",
    }
    return out

def main():
    cfg=yaml.safe_load((ROOT/"config.yaml").read_text(encoding="utf-8"))
    markets=[m["key"] for m in cfg["markets"]]
    rows=[r for k in markets if (r:=summarize_market(k))]
    if not rows: return
    with open(SUM/"markets_summary.csv","w",newline="",encoding="utf-8") as f:
        w=csv.DictWriter(f, fieldnames=rows[0].keys()); w.writeheader(); w.writerows(rows)
    print("Skrev summaries/markets_summary.csv")

if __name__=="__main__":
    main()
