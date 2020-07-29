# BigQuery UniView

Create BigQuery views that unify sets of table with the same prefix and
different versions. The view returns a union of all records from the
different table versions and cast columns to a common data type.

This tool was written to be deployed as a Cloud Function. The function is
triggered by a Pub/Sub topic feeded by our BigQuery sink. The dispatched
messages contain information about table that are created by our process.

e.g:
```
{"dataset":"...","table":{"name":"customer","version":"5b6ffe5fb"}}
```

## Rules

Columns that conflict in their data type are cast to a common representation.
The specific cast used depends on the conflicting data types. Basic idea is
to extend the target data type. We go from a more specific to more generic
type. This mitigates any information loss. As such, STRING represents our most
generic type, since it can represent any other BQ type.

| Conflict Types     | Cast To   |
|--------------------|-----------|
| DATETIME TIMESTAMP | TIMESTAMP |
| ANY ANY            | STRING    |

## Variables

The `PROJECT_ID` environment variable must be set to a valid Google Project ID.
The service account must have READ access to the source dataset and WRITE access
to the output dataset.

## Deployment

The following example is the command that we use to deploy on GCP:

```
gcloud functions deploy --project <PROJECT_ID> --runtime python37 --retry \
    --entry-point handler --source ./ --trigger-topic bigquery-table-create \
    bigquery-uniview --set-env-vars PROJECT_ID=<PROJECT_ID> \
    --service-account bigquery-uniview@<PROJECT_ID>.iam.gserviceaccount.com
```
