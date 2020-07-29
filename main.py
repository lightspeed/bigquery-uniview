import base64
import json
import os

from google.cloud import bigquery

from render import convert_rows_to_tables, render

project_id = os.getenv("PROJECT_ID")
client = bigquery.Client(project=project_id)


def handler(event: dict, _: dict) -> None:
    dataset_name, table_name = _extract_data(event)

    metadata_query = f'''
    WITH
    /* Get tables with all associated column names */
    tab2col AS (
        SELECT
            table_name,
            ARRAY_AGG(DISTINCT column_name) AS column_names
        FROM
            `{project_id}`.{dataset_name}.INFORMATION_SCHEMA.COLUMNS
        WHERE
            table_name LIKE "{table_name}\\\\_v\\\\_%"
        GROUP BY
            table_name),
    /* Get columns with all associated data types */
    col2typ AS (
        SELECT
            column_name,
            ARRAY_AGG(DISTINCT data_type) AS data_types
        FROM
            `{project_id}`.{dataset_name}.INFORMATION_SCHEMA.COLUMNS
        WHERE
            table_name LIKE "{table_name}\\\\_v\\\\_%"
        GROUP BY
            column_name)
    /* Get combination of table names with column definitions */
    SELECT
        tab2col.table_name,
        ARRAY(
            SELECT AS STRUCT
                col2typ.column_name AS name,
                col2typ.data_types AS types
            FROM
                UNNEST(tab2col.column_names) AS col_name
            JOIN
                col2typ ON col_name = col2typ.column_name) AS cols
    FROM
        tab2col;'''

    rows = client.query(metadata_query)
    table_versions = convert_rows_to_tables(rows)

    view_query = render(project_id, dataset_name, table_name, table_versions)
    client.query(view_query).result()


def _extract_data(event: dict) -> (str, str):
    data = json.loads(base64.b64decode(event['data']).decode('utf-8'))

    return data['dataset'], data['table']['name']
