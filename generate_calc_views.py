"""
generate_calc_views.py
======================
Assembles individual micro-calc SQL files into unified BigQuery views.

Mirrors the AO microtools CI/CD pattern:
  - Each subfolder of models/Calc_Anchorage/ is a calc group (e.g. CustodyPositionsCalc/)
  - Each .sql file inside is one metric (e.g. PositionPeriod.sql)
  - This script reads all .sql files for a group, resolves Jinja source() references,
    and creates a single CREATE OR REPLACE VIEW in Calc_Anchorage that joins them all.

Run BEFORE dbt run:
  python generate_calc_views.py          # execute in BigQuery
  python generate_calc_views.py --dry-run  # print SQL only, no BigQuery calls

Example output for CustodyPositionsCalc:
  CREATE OR REPLACE VIEW `project.Calc_Anchorage.CustodyPositionsCalc` AS
  WITH PositionPeriod AS (
    SELECT CustodyPositions.ID AS ID
         , DATE_TRUNC(...) AS PositionPeriod
    FROM `project.Semantic_Anchorage.CustodyPositions` AS CustodyPositions
  )
  SELECT
    Base.ID
    , PositionPeriod.PositionPeriod
  FROM `project.Semantic_Anchorage.CustodyPositions` AS Base
  LEFT JOIN PositionPeriod ON PositionPeriod.ID = Base.ID
"""

import re
import sys
from pathlib import Path

PROJECT = 'sigma-method-453023-a4'
SEMANTIC_DATASET = 'Semantic_Anchorage'
CALC_DATASET = 'Calc_Anchorage'
CALC_ROOT = Path(__file__).parent / 'models' / 'Calc_Anchorage'

# Maps dbt source() names to BigQuery dataset paths
SOURCE_MAP = {
    'anchorage_data_platform': f'{PROJECT}.{SEMANTIC_DATASET}',
}


def resolve_jinja_sources(sql: str) -> str:
    """Replace {{ source('source_name', 'TableName') }} with fully-qualified BQ names."""

    def replacer(match):
        src_name = match.group(1)
        table_name = match.group(2)
        bq_dataset = SOURCE_MAP.get(src_name)
        if bq_dataset is None:
            raise ValueError(f"Unknown source reference: '{src_name}'. Add to SOURCE_MAP.")
        return f'`{bq_dataset}.{table_name}`'

    return re.sub(
        r'\{\{\s*source\(\s*[\'"]([^\'"]+)[\'"]\s*,\s*[\'"]([^\'"]+)[\'"]\s*\)\s*\}\}',
        replacer,
        sql,
    )


def build_view_sql(calc_dir: Path) -> str | None:
    """
    Given a calc group directory (e.g. models/Calc_Anchorage/CustodyPositionsCalc),
    return the CREATE OR REPLACE VIEW DDL that assembles all micro-calcs inside it.

    Returns None if the directory contains no .sql files.
    """
    folder_name = calc_dir.name  # e.g. CustodyPositionsCalc
    # Derive base semantic table name: strip trailing 'Calc'
    base_table = folder_name[:-4] if folder_name.endswith('Calc') else folder_name
    base_fq = f'`{PROJECT}.{SEMANTIC_DATASET}.{base_table}`'
    view_fq = f'`{PROJECT}.{CALC_DATASET}.{folder_name}`'

    sql_files = sorted(calc_dir.glob('*.sql'))
    if not sql_files:
        return None

    calc_names = [f.stem for f in sql_files]
    cte_parts = []
    for sql_file in sql_files:
        raw = sql_file.read_text().strip()
        resolved = resolve_jinja_sources(raw)
        # Indent body 2 spaces inside the CTE parentheses
        indented = '\n'.join('  ' + line for line in resolved.splitlines())
        cte_parts.append(f'{sql_file.stem} AS (\n{indented}\n)')

    with_clause = 'WITH ' + '\n, '.join(cte_parts)

    select_lines = ['  Base.ID'] + [f'  , {name}.{name}' for name in calc_names]
    select_str = 'SELECT\n' + '\n'.join(select_lines)

    join_lines = [f'LEFT JOIN {name}\n  ON {name}.ID = Base.ID' for name in calc_names]
    from_str = f'FROM {base_fq} AS Base\n' + '\n'.join(join_lines)

    return f'CREATE OR REPLACE VIEW {view_fq} AS\n{with_clause}\n{select_str}\n{from_str}\n'


def main(dry_run: bool = False) -> None:
    if not dry_run:
        from google.cloud import bigquery  # noqa: PLC0415
        client = bigquery.Client(project=PROJECT)

    calc_dirs = sorted(d for d in CALC_ROOT.iterdir() if d.is_dir())
    if not calc_dirs:
        print(f'No subdirectories found under {CALC_ROOT}')
        return

    for calc_dir in calc_dirs:
        view_sql = build_view_sql(calc_dir)
        if view_sql is None:
            print(f'  Skipping {calc_dir.name} — no .sql files found')
            continue

        view_name = f'{PROJECT}.{CALC_DATASET}.{calc_dir.name}'
        print(f'\nBuilding `{view_name}`...')

        if dry_run:
            print(view_sql)
        else:
            job = client.query(view_sql)
            job.result()
            print(f'  ✓ Created `{view_name}`')

    print('\nDone.')


if __name__ == '__main__':
    dry_run = '--dry-run' in sys.argv
    main(dry_run=dry_run)
