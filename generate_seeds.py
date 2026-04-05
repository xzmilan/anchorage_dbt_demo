#!/usr/bin/env python3
"""
Seed data generator for Anchorage Digital Reporting & Statements demo.

Generates ~1,430 rows of realistic institutional crypto custody/staking data:
  - 20  customers (hedge funds, pensions, endowments, sovereign wealth)
  - 10  reference assets (BTC, ETH, SOL, DOT, ADA, ATOM, AVAX, MATIC, USDC, USDT)
  - 300 raw custody position snapshots
  - 400 raw staking reward events  (settled + net pending / NetPen)
  - 500 raw canonical financial events
  - 200 raw fee events

Usage:
    python generate_seeds.py
"""

import csv
import random
import json
from datetime import datetime, timedelta, date
from pathlib import Path

# --- reproducible output ---
random.seed(42)

SEEDS_DIR = Path("seeds")
SEEDS_DIR.mkdir(exist_ok=True)

# ──────────────────────────────────────────────────────────────── reference data ──

CUSTOMERS = [
    ("CUST001", "Blackstone Digital Assets Fund",   "hedge_fund",       "US", "2022-01-15", "approved",       "tier_1"),
    ("CUST002", "Fidelity Crypto Strategies LP",    "hedge_fund",       "US", "2022-03-10", "approved",       "tier_1"),
    ("CUST003", "GIC Singapore Digital",            "sovereign_wealth",  "SG", "2022-06-01", "approved",       "tier_1"),
    ("CUST004", "CalPERS Digital Assets",           "pension_fund",     "US", "2022-08-20", "approved",       "tier_1"),
    ("CUST005", "ARK Digital Asset Fund",           "asset_manager",    "US", "2022-09-05", "approved",       "tier_2"),
    ("CUST006", "Yale Endowment Digital",           "endowment",        "US", "2022-11-01", "approved",       "tier_1"),
    ("CUST007", "Bridgewater Crypto Alpha",         "hedge_fund",       "US", "2023-01-10", "approved",       "tier_2"),
    ("CUST008", "ADIA Digital Assets",              "sovereign_wealth",  "AE", "2023-02-15", "approved",       "tier_1"),
    ("CUST009", "Harvard Management Digital",       "endowment",        "US", "2023-03-20", "approved",       "tier_1"),
    ("CUST010", "Grayscale Institutional LLC",      "asset_manager",    "US", "2023-04-01", "approved",       "tier_2"),
    ("CUST011", "Tiger Global Crypto Fund",         "hedge_fund",       "US", "2023-05-10", "approved",       "tier_2"),
    ("CUST012", "Ontario Teachers Digital",         "pension_fund",     "CA", "2023-06-15", "approved",       "tier_1"),
    ("CUST013", "A16Z Digital Fund III",            "venture_capital",  "US", "2023-07-01", "approved",       "tier_2"),
    ("CUST014", "Swiss Re Digital Assets",          "insurance",        "CH", "2023-08-20", "approved",       "tier_2"),
    ("CUST015", "Nexus Quant Crypto LP",            "hedge_fund",       "US", "2023-09-05", "approved",       "tier_3"),
    ("CUST016", "Nordic Pension Digital",           "pension_fund",     "SE", "2023-10-10", "approved",       "tier_2"),
    ("CUST017", "Commonwealth Digital Trust",       "endowment",        "US", "2023-11-15", "approved",       "tier_3"),
    ("CUST018", "DeFi Quant Partners",              "hedge_fund",       "US", "2024-01-10", "pending_review", "tier_3"),
    ("CUST019", "Wellington Digital Growth",        "asset_manager",    "US", "2024-02-20", "approved",       "tier_2"),
    ("CUST020", "Macro Digital Strategies",         "hedge_fund",       "US", "2024-03-01", "approved",       "tier_3"),
]

# (asset_id, symbol, asset_name, asset_type, protocol, is_stakeable, min_stake, unbonding_days, coingecko_id)
ASSETS = [
    ("ASSET_BTC",  "BTC",  "Bitcoin",   "custody_only", "Bitcoin",   False,  0.0,  0,  "bitcoin"),
    ("ASSET_ETH",  "ETH",  "Ethereum",  "staking",      "Ethereum",  True,  32.0,  0,  "ethereum"),
    ("ASSET_SOL",  "SOL",  "Solana",    "staking",      "Solana",    True,   1.0,  2,  "solana"),
    ("ASSET_DOT",  "DOT",  "Polkadot",  "staking",      "Polkadot",  True,  10.0, 28,  "polkadot"),
    ("ASSET_ADA",  "ADA",  "Cardano",   "staking",      "Cardano",   True,  10.0,  0,  "cardano"),
    ("ASSET_ATOM", "ATOM", "Cosmos",    "staking",      "Cosmos",    True,   1.0, 21,  "cosmos"),
    ("ASSET_AVAX", "AVAX", "Avalanche", "staking",      "Avalanche", True,  25.0,  0,  "avalanche-2"),
    ("ASSET_MATIC","MATIC","Polygon",   "staking",      "Polygon",   True,   1.0,  3,  "matic-network"),
    ("ASSET_USDC", "USDC", "USD Coin",  "stablecoin",   "Ethereum",  False,  0.0,  0,  "usd-coin"),
    ("ASSET_USDT", "USDT", "Tether",    "stablecoin",   "Ethereum",  False,  0.0,  0,  "tether"),
]

ASSET_PRICES = {
    "BTC": 65000, "ETH": 3500, "SOL": 180,  "DOT": 8,
    "ADA": 0.45,  "ATOM": 12,  "AVAX": 35,  "MATIC": 0.85,
    "USDC": 1.0,  "USDT": 1.0,
}

VALIDATOR_ADDRESSES = {
    "ETH":  ["0xae7ab96520de3a18e5e111b5eaab095312d7fe84",
             "0x00000000219ab540356cbb839cbe05303d7705fa"],
    "SOL":  ["Vote111111111111111111111111111111111111111",
             "GE6atKoWiQ2pt3zL7N13pjNHjdLVys8LinG8qeJLcAiC"],
    "DOT":  ["12xtAYsRUrmbniiWQqJtECiBQrMn8AypQcXhnQAc6RB6XkLW",
             "16SpacegeUTft9v3ts27CEC3tJaxgvE4uZeCctThFH3Vb35T"],
    "ADA":  ["pool1pu5jlj4q9w9jlxeu0z42304fyjrjw36jqsjttxzmlkelh2vqe"],
    "ATOM": ["cosmosvaloper1clpqr4nrk4khgkxj78fcwwh6dl3uw4epsluffn"],
    "AVAX": ["NodeID-7Xhw2mDxuDS44j42TCB6U5579esbSt3Lg",
             "NodeID-MFrZFVCXPv5iCn6M9K6XduxGTYp891xXZ"],
    "MATIC":["0x3f5ce5fbfe3e9af3971dd833d26ba9b5c936f0be"],
}

STAKEABLE = [a for a in ASSETS if a[5]]
STABLECOINS = [a for a in ASSETS if a[3] == "stablecoin"]

# 12-month reporting window: Jul 2024 – Jun 2025
PERIOD_START = date(2024, 7, 1)
PERIOD_END   = date(2025, 6, 30)

PERIODS = []
d = PERIOD_START
while d <= PERIOD_END:
    PERIODS.append((d.year, d.month))
    d = date(d.year + (d.month // 12), (d.month % 12) + 1, 1)


# ──────────────────────────────────────────────────────────────── helpers ──

def rand_date(start: date, end: date) -> date:
    return start + timedelta(days=random.randint(0, (end - start).days))

def rand_ts(start: date, end: date) -> str:
    d = rand_date(start, end)
    return f"{d} {random.randint(0,23):02d}:{random.randint(0,59):02d}:{random.randint(0,59):02d}"

def write_csv(path: Path, headers: list, rows: list):
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(headers)
        w.writerows(rows)


# ──────────────────────────────────────────────────────────────── generators ──

def gen_customers():
    rows = [[*c, True] for c in CUSTOMERS]
    write_csv(SEEDS_DIR / "customers.csv",
              ["customer_id","customer_name","customer_type","jurisdiction",
               "onboarded_date","kyc_status","risk_tier","is_active"],
              rows)
    print(f"✓ customers.csv              ({len(rows):>4} rows)")


def gen_ref_assets():
    write_csv(SEEDS_DIR / "ref_assets.csv",
              ["asset_id","symbol","asset_name","asset_type","protocol",
               "is_stakeable","min_stake_amount","unbonding_days","coingecko_id"],
              ASSETS)
    print(f"✓ ref_assets.csv             ({len(ASSETS):>4} rows)")


def gen_custody_positions():
    cust_assets = {c[0]: random.sample(ASSETS, random.randint(4, 8)) for c in CUSTOMERS}
    rows = []
    pos_id = 1
    for cust in CUSTOMERS:
        cid = cust[0]
        for year, month in PERIODS:
            eom = date(year, month, 28)
            pd_ = date(year, month, 1)
            for asset in cust_assets[cid]:
                sym = asset[1]
                if sym == "BTC":
                    qty = round(random.uniform(5, 500), 8)
                elif sym in ("ETH", "SOL"):
                    qty = round(random.uniform(50, 5000), 8)
                elif sym in ("USDC", "USDT"):
                    qty = round(random.uniform(100_000, 50_000_000), 2)
                else:
                    qty = round(random.uniform(100, 100_000), 4)
                price = round(ASSET_PRICES[sym] * random.uniform(0.92, 1.08), 2)
                rows.append([
                    f"POS{pos_id:06d}", cid, asset[0],
                    qty, price, round(qty * price, 2),
                    eom.isoformat(),
                    f"CUSTREF-{cid}-{sym}-{year}{month:02d}",
                    rand_ts(pd_, eom),
                ])
                pos_id += 1

    random.shuffle(rows)
    rows = rows[:300]
    write_csv(SEEDS_DIR / "raw_custody_positions.csv",
              ["position_id","customer_id","asset_id","quantity","price_usd",
               "valuation_usd","position_date","custodian_ledger_ref","created_at"],
              rows)
    print(f"✓ raw_custody_positions.csv  ({len(rows):>4} rows)")


def gen_staking_events():
    rows = []
    evt_id = 1
    for cust in CUSTOMERS:
        cid = cust[0]
        portfolio = random.sample(STAKEABLE, random.randint(2, min(5, len(STAKEABLE))))
        for asset in portfolio:
            sym         = asset[1]
            unbonding   = asset[7]
            validators  = VALIDATOR_ADDRESSES.get(sym, ["validator_default_addr"])
            for year, month in PERIODS:
                pstart = date(year, month, 1)
                pend   = date(year, month, 28)
                for _ in range(random.randint(2, 5)):
                    earn_dt = rand_date(pstart, pend)
                    if unbonding == 0:
                        settle_dt = earn_dt + timedelta(days=random.randint(0, 2))
                        state = "settled"
                    else:
                        settle_dt = earn_dt + timedelta(days=unbonding)
                        state = "settled" if settle_dt <= PERIOD_END else "pending"

                    dist_dt = None if state == "pending" else settle_dt + timedelta(days=random.randint(0, 3))

                    if sym == "ETH":
                        gross = round(random.uniform(0.001, 0.1), 8)
                    elif sym == "SOL":
                        gross = round(random.uniform(0.01, 2.0), 8)
                    elif sym in ("DOT", "ATOM"):
                        gross = round(random.uniform(0.1, 10.0), 8)
                    else:
                        gross = round(random.uniform(1.0, 100.0), 8)

                    fee = round(gross * random.uniform(0.08, 0.12), 8)
                    net = round(gross - fee, 8)

                    rows.append([
                        f"STAKE{evt_id:06d}", cid, asset[0],
                        random.choice(validators), "reward_earned",
                        gross, fee, net,
                        earn_dt.isoformat(),
                        settle_dt.isoformat() if state == "settled" else "",
                        dist_dt.isoformat()   if dist_dt else "",
                        state,
                        f"epoch_{random.randint(100_000, 999_999)}",
                        str(random.randint(15_000_000, 20_000_000)),
                        "protocol_oracle",
                        rand_ts(pstart, pend),
                    ])
                    evt_id += 1

    random.shuffle(rows)
    rows = rows[:400]
    write_csv(SEEDS_DIR / "raw_staking_events.csv",
              ["event_id","customer_id","asset_id","validator_address","event_type",
               "gross_amount","fee_amount","net_amount",
               "earn_date","settle_date","distribute_date","reward_state",
               "protocol_epoch","protocol_block_height","source_system","created_at"],
              rows)
    print(f"✓ raw_staking_events.csv     ({len(rows):>4} rows)")


def gen_financial_events():
    EVENT_CATALOG = [
        ("custody_deposit",          "custody_platform", "custody",    0.0010),
        ("custody_withdrawal",        "custody_platform", "custody",    0.0010),
        ("staking_reward_earned",     "protocol_oracle",  "staking",    0.1000),
        ("staking_reward_settled",    "protocol_oracle",  "staking",    0.1000),
        ("custody_fee_charge",        "billing_system",   "custody",    0.0200),
        ("staking_fee_charge",        "billing_system",   "staking",    0.1000),
        ("stablecoin_transfer_in",    "custody_platform", "stablecoin", 0.0005),
        ("stablecoin_transfer_out",   "custody_platform", "stablecoin", 0.0005),
    ]

    rows = []
    evt_id = 1
    for cust in CUSTOMERS:
        cid = cust[0]
        sel_assets = random.sample(ASSETS, random.randint(4, 8))
        for year, month in random.sample(PERIODS, random.randint(4, 12)):
            pstart = date(year, month, 1)
            pend   = date(year, month, 28)
            for _ in range(random.randint(3, 8)):
                etype, source, product, fee_rate = random.choice(EVENT_CATALOG)
                asset = random.choice(sel_assets)

                # keep event type and asset compatible
                if "staking" in etype:
                    asset = random.choice(STAKEABLE)
                elif "stablecoin" in etype:
                    asset = random.choice(STABLECOINS)

                sym   = asset[1]
                gross = (round(random.uniform(10_000, 5_000_000), 2) if "stablecoin" in etype
                         else round(random.uniform(0.01, 10.0), 8) if sym == "BTC"
                         else round(random.uniform(0.1, 100.0), 8)  if sym in ("ETH","SOL")
                         else round(random.uniform(1.0, 1_000.0), 8))
                fee   = round(gross * fee_rate, 8)
                net   = round(gross - fee, 8)

                settle_dt = (pend + timedelta(days=random.randint(0, 5))
                             if "staking" in etype
                             else pstart + timedelta(days=random.randint(0, 3)))
                status = "settled" if settle_dt <= PERIOD_END else "pending"
                ts     = rand_ts(pstart, pend)

                rows.append([
                    f"FE{evt_id:06d}", cid, asset[0], etype,
                    gross, fee, net,
                    ts,
                    f"{year}-{month:02d}-01",
                    settle_dt.isoformat(),
                    status, source, product,
                    json.dumps({
                        "product_version": "v2",
                        "protocol_ref": f"TX_{random.randint(100_000,999_999)}",
                        "custodian_ref": f"CREF_{random.randint(100_000,999_999)}",
                    }),
                    ts,
                ])
                evt_id += 1

    random.shuffle(rows)
    rows = rows[:500]
    write_csv(SEEDS_DIR / "raw_financial_events.csv",
              ["event_id","customer_id","asset_id","event_type",
               "gross_amount","fee_amount","net_amount",
               "event_timestamp","period_date","settlement_date",
               "status","source_system","product_line","metadata_json","created_at"],
              rows)
    print(f"✓ raw_financial_events.csv   ({len(rows):>4} rows)")


def gen_fee_events():
    FEE_TYPES = [
        ("custody_fee",           15),
        ("staking_fee",          100),
        ("premium_reporting_fee", 5),
    ]
    rows = []
    fee_id = 1
    for cust in CUSTOMERS:
        cid  = cust[0]
        tier = cust[6]
        sel  = random.sample(ASSETS, random.randint(3, 6))
        for year, month in PERIODS:
            pstart = date(year, month, 1)
            pend   = date(year, month, 28)
            for asset in sel:
                ftype, bps = random.choice(FEE_TYPES)
                if ftype == "staking_fee" and not asset[5]:
                    continue
                sym = asset[1]
                aum = (random.uniform(5_000_000, 500_000_000) if tier == "tier_1"
                       else random.uniform(500_000, 50_000_000) if tier == "tier_2"
                       else random.uniform(50_000, 5_000_000))
                fee_usd = round(aum * bps / 10_000 / 12, 2)
                rows.append([
                    f"FEE{fee_id:06d}", cid, asset[0], ftype, bps,
                    fee_usd, round(aum, 2),
                    pstart.isoformat(), pend.isoformat(),
                    "billed",
                    f"INV-{year}{month:02d}-{cid}-{sym}",
                    rand_ts(pend, pend + timedelta(days=5)),
                ])
                fee_id += 1

    random.shuffle(rows)
    rows = rows[:200]
    write_csv(SEEDS_DIR / "raw_fee_events.csv",
              ["fee_id","customer_id","asset_id","fee_type","basis_points",
               "fee_amount_usd","aum_at_billing",
               "fee_period_start","fee_period_end",
               "billing_status","invoice_reference","created_at"],
              rows)
    print(f"✓ raw_fee_events.csv         ({len(rows):>4} rows)")


if __name__ == "__main__":
    print("Generating seed data for Anchorage Reporting & Statements demo...\n")
    gen_customers()
    gen_ref_assets()
    gen_custody_positions()
    gen_staking_events()
    gen_financial_events()
    gen_fee_events()
    total = 20 + 10 + 300 + 400 + 500 + 200
    print(f"\n✓ Done — {total} total rows across 6 seed files.")
    print("Next: dbt seed --profiles-dir . && dbt run --profiles-dir . && dbt test --profiles-dir .")
