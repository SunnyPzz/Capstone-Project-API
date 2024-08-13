from airflow.decorators import dag, task
from datetime import datetime, timedelta
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from google.cloud import bigquery
from astro import sql as aql
from astro.files import File
from astro.sql.table import Table, Metadata
from astro.constants import FileType

PROJECT_ID = 'My First Project'
MY_BUCKET = 'de-project-sunny1'
CONN_ID = 'gcp'


@dag(
    start_date=datetime(2024,1,1),
    schedule=None,
    catchup=False,
    tags=['retail']
)


def retail():
    
    upload_csv_to_gcs = LocalFilesystemToGCSOperator(
            task_id='upload_csv_to_gcs',
            src='/usr/local/airflow/include/dataset/online_retail.csv',
            dst='raw/online_retail.csv',
            bucket=MY_BUCKET,
            gcp_conn_id="gcp",
            mime_type="text/csv",
    )

    create_empty_dataset = BigQueryCreateEmptyDatasetOperator(
            task_id='create_empty_dataset',
            dataset_id='first_project_retail',
            gcp_conn_id=CONN_ID,
            exists_ok=True,
    )

    schema = [
    {"name": "InvoiceNo", "type": "STRING"},
    {"name": "StockCode", "type": "STRING"},
    {"name": "Description", "type": "STRING"},
    {"name": "Quantity", "type": "INTEGER"},
    {"name": "InvoiceDate", "type": "STRING"},
    {"name": "UnitPrice", "type": "FLOAT"},
    {"name": "CustomerID", "type": "FLOAT"},
    {"name": "Country", "type": "STRING"},
    ]

    create_empty_table = BigQueryCreateEmptyTableOperator(
        task_id='create_empty_table',
        dataset_id='first_project_retail',
        gcp_conn_id=CONN_ID,
        table_id='raw_invoices',
        exists_ok=False, 
        schema_fields=schema,
    )

    load_file_from_gcs_to_bq = GCSToBigQueryOperator(
        task_id='load_file_from_gcs_to_bq',
        bucket=MY_BUCKET,
        destination_project_dataset_table='first_project_retail.raw_invoices',
        source_objects='raw/online_retail.csv',
        source_format='CSV',
        autodetect="False",
        skip_leading_rows = 1,
        gcp_conn_id=CONN_ID,
    )

    # gcs_to_raw = aql.load_file(
    #     task_id='gcs_to_raw',
    #     input_file=File('gs://de-project-sunny1/raw/online_retail.csv',
    #                     conn_id='gcp',
    #                     filetype=FileType.CSV,),
    #     output_table=Table(
    #         name='raw_invoices',
    #         conn_id='gcp',
    #         metadata=Metadata(schema='retail')
    #     ),
    #     use_native_support=False,
    # )

    upload_csv_to_gcs >> create_empty_dataset >> create_empty_table >> load_file_from_gcs_to_bq

retail()