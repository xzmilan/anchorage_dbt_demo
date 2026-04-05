# Anchorage Digital — Semantic Layer Architecture Demo

Stack: **Python · dbt Core · BigQuery · GitHub Actions · SQLFluff**
GCP Project: `sigma-method-453023-a4`
Contact: [xzmilan@gmail.com](mailto:xzmilan@gmail.com) · [linkedin.com/in/stevenpassanante](https://linkedin.com/in/stevenpassanante)

---

## What This Demonstrates

This is a two-repo system that models how Anchorage Digital could structure a production semantic layer for institutional client reporting — custody
positions, staking rewards, fee accruals, and SOX-grade reconciliation.

The two repos mirror the AO team structure:

| Repo | Layer | Owner |
|---|---|---|
| [`anchorage_data_platform`](../anchorage_data_platform) | `Semantic_Anchorage` | Platform team |
| this repo | `Calc_Anchorage` · `Widetable_Anchorage` · `Business_View_Anchorage` | Asset, Finance, Reporting teams |

---

## The Four-Tier Architecture

```
Raw Seeds (BigQuery: raw)
    │
    ▼
Semantic_Anchorage     ← anchorage_data_platform repo (Platform team)
    │  Hashed IDs, enforced contracts, no business logic
    │  6 tables: Customers, ReferenceAssets, CustodyPositions,
    │            StakingEvents, FeeEvents, FinancialEvents
    │
    ▼
Calc_Anchorage         ← this repo / models/Calc_Anchorage/ (domain teams)
    │  1 file = 1 metric = 2 fields only: ID + the calc
    │  CI/CD assembles micro-calcs into BigQuery views via generate_calc_views.py
    │  CustodyPositionsCalc: PositionPeriod
    │  StakingEventsCalc:    EarnPeriod, IsSettled, UnbondingCategory
    │  FeeEventsCalc:        FeePeriod, IsFullyBilled
    │  FinancialEventsCalc:  EventPeriod, IsPending
    │
    ▼
Widetable_Anchorage    ← this repo / models/Widetable_Anchorage/ (CI/CD mechanical)
    │  Struct SELECT only: SELECT Base, BaseCalc FROM ... JOIN ... ON ID
    │  No explicit columns. No logic. Automatically picks up every new calc.
    │  CustodyPositions, StakingEvents, FeeEvents, FinancialEvents
    │
    ▼
Business_View_Anchorage ← this repo / models/Business_View_Anchorage/ (Reporting team)
    Aggregated, client-facing, period-aligned
    CustodyBalancesByPeriod, StakingRewardsByPeriod, FeeAccrualsByPeriod,
    ClientPeriodPositions, ReconciliationSummary
```

---

## Why This Design

### 1. Stability at every boundary
The semantic base layer is immutable from a downstream perspective. Platform team owns
the contract. If `CustodyPositions` changes a column name or type, the enforced dbt contract
fails their CI before a single downstream model is affected. Analytics teams can evolve
calc logic freely without touching the base layer.

### 2. The micro-calc pattern eliminates merge conflicts
In a traditional architecture, 3 analysts adding metrics to the same file causes constant
merge conflicts. Here, each metric is its own file. Asset team adds `IsSettled.sql`. Finance
team adds `IsFullyBilled.sql`. They never touch the same file. `generate_calc_views.py`
assembles both into `StakingEventsCalc` at deploy time automatically.

### 3. Widetables are zero-maintenance
A widetable is `SELECT Base, BaseCalc FROM base JOIN calc ON ID`. No column list.
When a new calc is added and assembled by CI/CD, the widetable picks it up automatically
at the next deploy. No one has to update the widetable. This is how AO's system handles
1,300+ metrics without the widetables becoming maintenance burdens.

### 4. Business views are pure aggregation — no joins to raw
All joins happen at the widetable layer. Business views only aggregate and filter.
This means the Reporting team can change how they aggregate staking rewards without
understanding custody position joins. The layers are genuinely independent.

### 5. CODEOWNERS enforces ownership automatically
The `asset-team` can only approve PRs to `StakingEventsCalc/`. They cannot merge changes
to `FeeEventsCalc/` or `Business_View_Anchorage/`. The data architect is required on
structural changes (new sources, widetable pattern, CI workflows). Compliance-gated views
require multi-team sign-off.

### 6. SQLFluff enforces the coding standard at the PR gate
No `SELECT *`. PascalCase aliases. Full table aliases. Leading commas. `ON` on its own
indented line. These aren't guidelines — they're enforced by CI and fail the PR if violated.

---

## Team Ownership Map

### Platform Team — `anchorage_data_platform` repo

**What they own:** Everything in `Semantic_Anchorage`. They are the single source of truth
for what raw data means. They define the hashed ID, the column names, the data types,
and the enforced contracts that all downstream teams depend on.

**What they do NOT own:** Any business logic. They don't know what `IsSettled` means
or how `FeePeriod` should be calculated. That's intentional.

**Their CI/CD gate:** If a contract column is renamed or dropped, their CI fails before
merge. Downstream teams are never surprised by a breaking schema change.

---

### Asset Team — `models/Calc_Anchorage/StakingEventsCalc/`

**What they own:** The protocol-specific logic for staking events. They are the only team
that knows:
- What `RewardState = 'SETTLED'` vs unbonding means for a given protocol
- That DOT has a 28-day unbonding period vs ETH post-merge with no unbonding
- Which epoch a reward belongs to for cross-protocol period alignment

**Their files:**
- `EarnPeriod.sql` — what month did this reward earn?
- `IsSettled.sql` — is this reward liquid and distributable?
- `UnbondingCategory.sql` — what is the client's unbonding risk exposure?

**What they do NOT own:** How those rewards are aggregated in `StakingRewardsByPeriod`.
That's the Reporting team's call. The Asset team produces clean, atomic fields. The
Reporting team decides how to use them.

---

### Reporting / Statements Team — `models/Business_View_Anchorage/`

**What they own:** The client-facing output. They define what appears on institutional
client statements and what internal compliance teams use to close month-end.

**Their views:**
- `ClientPeriodPositions` — the fully denormalized reporting spine. One row per
  customer/asset/period. All three teams' data unified. `TotalPositionValueWithNetpenUSD`
  is the key institutional metric: liquid position + unrealized pending rewards.
- `CustodyBalancesByPeriod` — period-end snapshot (latest position per month)
- `StakingRewardsByPeriod` — settled vs pending split, FeeTakeRatePct
- `FeeAccrualsByPeriod` — billed vs accrued per fee type, compliance gate
- `ReconciliationSummary` — SOX-grade reconciliation status. Priority order:
  MissingPosition → FeeWithoutBalance → HasPendingEvents → UnbilledFees → Reconciled.
  All rows must be `Reconciled` before month-end statements are delivered.

**What they do NOT own:** Protocol logic (that's the Asset team) or fee calculation
rules (that's the Finance team). They consume clean widetable structs and aggregate.

---

## CI/CD Pipeline

### On every Pull Request → [dbt-pull-request.yml](.github/workflows/dbt-pull-request.yml)

| Job | What it does |
|---|---|
| `changed-models` | `git diff` to detect which model files changed on this branch |
| `test-changed` | `generate_calc_views.py` → `dbt run` → `dbt test` on changed models only |
| `code-quality` | SQLFluff lint per layer + yamllint + per-rule violation report artifact |
| `dependencies` | Validates `requirements.txt` installs cleanly |

SQLFluff runs with `--templater jinja` in CI. This is deliberate: the `dbt` templater
requires a live BigQuery connection to cache relations, which fails in CI without
credentials. The Jinja templater processes `{{ source() }}` and `{{ ref() }}` as plain
variables — full lint coverage with no database dependency.

### On merge to main → [dbt-ci-cd.yml](.github/workflows/dbt-ci-cd.yml)

```
build (dbt compile)  ──┐
test  (dbt test)     ──┼──→  deploy (generate_calc_views.py → dbt run → dbt test)
```

Deploy runs only after both `build` and `test` pass. The deploy step runs
`generate_calc_views.py` first to assemble the micro-calc BigQuery views in
`Calc_Anchorage`, then `dbt run` builds the Widetable and Business_View layers on top.

---

## The Calc Assembly Pattern

`generate_calc_views.py` at the root of this repo replicates the AO microtools CI/CD
assembly mechanism. It:

1. Reads every `.sql` file in each `*Calc/` subfolder under `models/Calc_Anchorage/`
2. Resolves Jinja `{{ source() }}` references to fully-qualified BigQuery table names
3. Assembles a `CREATE OR REPLACE VIEW` that wraps all micro-calcs in CTEs and
   LEFT JOINs them on `ID` back to the semantic base
4. Executes against BigQuery (`--dry-run` flag available for inspection)

```bash
# Dry-run — prints generated SQL, no BigQuery calls
python generate_calc_views.py --dry-run

# Execute — creates/replaces all calc views in Calc_Anchorage
python generate_calc_views.py
```

Adding a new metric requires only:
1. Create a `.sql` file in the appropriate `*Calc/` subfolder
2. Open a PR — `generate_calc_views.py` runs automatically in CI

---

## SQL Standards

Enforced by SQLFluff ([`.sqlfluff`](.sqlfluff)) on every commit (pre-commit) and every PR (CI):

| Rule | Standard |
|---|---|
| `CP01` | SQL keywords UPPERCASE |
| `CP02` | Aliases PascalCase — `TotalPositionValueWithNetpenUSD` not `total_position_value` |
| `CP04` | Boolean literals `TRUE` / `FALSE` |
| `AL01` / `AL02` | Explicit `AS` required on all table and column aliases |
| `LT04` | Leading commas on multi-line `SELECT` |
| `AM05` | Fully qualified `JOIN` syntax |
| `ST05` | No subqueries in `JOIN`s — use CTEs |
| No `SELECT *` | Explicit column lists everywhere, including inside CTEs |
| Full table aliases | `FROM raw_custody_positions AS RawCustodyPositions` — never single letters |
| `ON` placement | On its own indented line after the `JOIN` line |

---

## Repository Structure

```
anchorage_dbt_demo/
├── models/
│   ├── sources.yml                      # Cross-project boundary — Semantic_Anchorage + Calc_Anchorage
│   ├── Calc_Anchorage/                  # Domain team micro-calcs (1 file = 1 metric)
│   │   ├── CustodyPositionsCalc/
│   │   │   └── PositionPeriod.sql
│   │   ├── StakingEventsCalc/
│   │   │   ├── EarnPeriod.sql
│   │   │   ├── IsSettled.sql
│   │   │   └── UnbondingCategory.sql
│   │   ├── FeeEventsCalc/
│   │   │   ├── FeePeriod.sql
│   │   │   └── IsFullyBilled.sql
│   │   └── FinancialEventsCalc/
│   │       ├── EventPeriod.sql
│   │       └── IsPending.sql
│   ├── Widetable_Anchorage/             # Mechanical struct SELECT — no logic
│   │   ├── CustodyPositions.sql
│   │   ├── StakingEvents.sql
│   │   ├── FeeEvents.sql
│   │   └── FinancialEvents.sql
│   └── Business_View_Anchorage/         # Client-facing aggregated views
│       ├── CustodyBalancesByPeriod.sql
│       ├── StakingRewardsByPeriod.sql
│       ├── FeeAccrualsByPeriod.sql
│       ├── ClientPeriodPositions.sql
│       └── ReconciliationSummary.sql    # SOX compliance gate
├── scripts/
│   └── lint_summary.py                  # Per-rule SQLFluff violation count table
├── .github/
│   ├── CODEOWNERS                       # Per-folder team ownership
│   └── workflows/
│       ├── dbt-pull-request.yml         # PR: lint + test changed models
│       └── dbt-ci-cd.yml               # Merge: assemble calcs → dbt run → deploy
├── generate_calc_views.py               # AO microtools assembly pattern
├── dbt_project.yml
├── profiles.yml
├── packages.yml
├── requirements.txt
├── .sqlfluff
└── .gitignore
```

---

## Running Locally

```bash
# 1. Clone
git clone <this-repo>
cd anchorage_dbt_demo

# 2. Set up Python environment
python3.12 -m venv .venv312 && source .venv312/bin/activate
pip install -r requirements.txt
pip install --force-reinstall "chardet==5.2.0"

# 3. Authenticate GCP
gcloud auth application-default login

# 4. Install dbt packages
dbt deps

# 5. Assemble calc views in BigQuery (runs before dbt)
python generate_calc_views.py

# 6. Run all layers
dbt run --profiles-dir .

# 7. Test
dbt test --profiles-dir .

# 8. Lint SQL
sqlfluff lint models/ --dialect bigquery --templater jinja
```

---

*Built by Yenny (Steven) Passanante — demonstrating 6 years of enterprise semantic layer governance applied to the Anchorage Digital reporting domain.*

---

