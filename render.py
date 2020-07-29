from typing import Iterator, List, Set

from google.cloud.bigquery import Row

from column import Column
from table import Table


def render(
        project_id: str,
        dataset_name: str,
        table_name: str,
        table_versions: List[Table]) -> str:
    def render_table(table: Table):
        def render_column(col: Column):
            if not table.has_column(col): return f'NULL AS {col.name}'
            if not col.cast_required(): return col.name

            return f'CAST({col.name} AS {col.resolved_type}) AS {col.name}'

        all_columns = _get_all_columns(table_versions)
        joined_columns = ', '.join(map(render_column, all_columns))

        return f'SELECT {joined_columns}' + \
            f'FROM `{project_id}`.{dataset_name}.{table.name}'

    joined_tables = ' UNION ALL '.join(map(render_table, table_versions))
    return 'CREATE OR REPLACE VIEW `{}`.{}_unified.{}_unified AS {}'.format(
        project_id, dataset_name, table_name, joined_tables)


def convert_rows_to_tables(rows: Iterator[Row]) -> List[Table]:
    def map_columns(cols) -> List[Column]:
        return [Column(c['name'], c['types']) for c in cols]

    return [Table(r.table_name, map_columns(r.cols)) for r in rows]


def _get_all_columns(tables: List[Table]) -> Set[Column]:
    return set(c for t in tables for c in t.columns)
