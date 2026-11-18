
import json
from datetime import datetime
from pathlib import Path

import google.auth

from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator


# =====================================================
# 1. PROJECT CONFIGURATION
# =====================================================

GCP_PROJECT = "data-engineering-b55"

GCS_BUCKET = "shopkart-data-pipeline-raw-20261117"

PROJECT_ROOT = Path("/opt/airflow/shopkart")

DATASETS = [
    "customers",
    "products",
    "orders",
    "order_items",
    "payments",
    "inventory_events",
]

BQ_LOCATION = "asia-south1"

BRONZE_DATASET = "shopkart_bronze"


# =====================================================
# 2. UPLOAD SOURCE DATA TO GCS
# =====================================================

def upload_to_gcs(**context):

    from google.cloud import storage

    # Airflow execution date
    ingestion_date = context["ds"]

    # Authenticate
    credentials, _ = google.auth.default(
        scopes=[
            "https://www.googleapis.com/auth/cloud-platform"
        ]
    )

    # Connect to GCS
    client = storage.Client(
        project=GCP_PROJECT,
        credentials=credentials
    )

    bucket = client.bucket(GCS_BUCKET)

    # Upload all six CSV files
    for dataset in DATASETS:

        print(f"Starting GCS upload: {dataset}")

        local_file = (
            PROJECT_ROOT
            / "data"
            / "raw"
            / f"{dataset}.csv"
        )

        # Verify local file
        if not local_file.exists():

            raise FileNotFoundError(
                f"File not found: {local_file}"
            )

        # GCS destination
        destination = (
            f"raw/{dataset}/"
            f"ingestion_date={ingestion_date}/"
            f"{dataset}.csv"
        )

        blob = bucket.blob(destination)

        # Prevent overwriting existing files
        if blob.exists(client):

            raise FileExistsError(
                f"GCS file already exists: {destination}"
            )

        # Upload file
        blob.upload_from_filename(
            str(local_file),
            content_type="text/csv",
            if_generation_match=0
        )

        # Verify upload
        blob.reload()

        print(
            f"SUCCESS: {dataset} | "
            f"Size: {blob.size:,} bytes"
        )

    print("All six datasets uploaded successfully!")


# =====================================================
# 3. LOAD DATA FROM GCS INTO BIGQUERY BRONZE
# =====================================================

def load_bronze_tables(**context):

    from google.cloud import bigquery, storage

    # Airflow execution date
    ingestion_date = context["ds"]

    # Authenticate
    credentials, _ = google.auth.default(
        scopes=[
            "https://www.googleapis.com/auth/cloud-platform"
        ]
    )

    # BigQuery client
    bq_client = bigquery.Client(
        project=GCP_PROJECT,
        location=BQ_LOCATION,
        credentials=credentials
    )

    # GCS client
    storage_client = storage.Client(
        project=GCP_PROJECT,
        credentials=credentials
    )

    bucket = storage_client.bucket(GCS_BUCKET)

    # Process all six datasets
    for dataset in DATASETS:

        print(f"Starting Bronze load: {dataset}")

        # -----------------------------------------
        # 3.1 GCS SOURCE FILE
        # -----------------------------------------

        object_name = (
            f"raw/{dataset}/"
            f"ingestion_date={ingestion_date}/"
            f"{dataset}.csv"
        )

        blob = bucket.blob(object_name)

        if not blob.exists(storage_client):

            raise FileNotFoundError(
                f"GCS file not found: "
                f"gs://{GCS_BUCKET}/{object_name}"
            )

        source_uri = (
            f"gs://{GCS_BUCKET}/{object_name}"
        )

        # -----------------------------------------
        # 3.2 READ JSON SCHEMA
        # -----------------------------------------

        schema_file = (
            PROJECT_ROOT
            / "config"
            / "schemas"
            / f"{dataset}_schema.json"
        )

        if not schema_file.exists():

            raise FileNotFoundError(
                f"Schema not found: {schema_file}"
            )

        with open(
            schema_file,
            "r",
            encoding="utf-8"
        ) as file:

            schema_data = json.load(file)

        # Support both JSON formats
        fields = (
            schema_data["fields"]
            if isinstance(schema_data, dict)
            else schema_data
        )

        # Build BigQuery schema
        schema = [
            bigquery.SchemaField(
                field["name"],
                field["type"],
                mode=field.get("mode", "NULLABLE")
            )
            for field in fields
        ]

        # -----------------------------------------
        # 3.3 BIGQUERY DESTINATION
        # -----------------------------------------

        table_id = (
            f"{GCP_PROJECT}."
            f"{BRONZE_DATASET}."
            f"{dataset}"
        )

        # -----------------------------------------
        # 3.4 LOAD JOB CONFIGURATION
        # -----------------------------------------

        
        job_config = bigquery.LoadJobConfig(

            source_format=bigquery.SourceFormat.CSV,

            skip_leading_rows=1,

            schema=schema,

            autodetect=False,

            write_disposition=(
                bigquery.WriteDisposition.WRITE_TRUNCATE
            ),

            allow_quoted_newlines=True,

        )


        # -----------------------------------------
        # 3.5 EXECUTE BIGQUERY LOAD JOB
        # -----------------------------------------

        load_job = bq_client.load_table_from_uri(

            source_uri,

            table_id,

            job_config=job_config,

            location=BQ_LOCATION

        )

        # Wait for completion
        load_job.result()

        # -----------------------------------------
        # 3.6 VERIFY BRONZE TABLE
        # -----------------------------------------

        table = bq_client.get_table(table_id)

        print(
            f"SUCCESS: {dataset} | "
            f"Rows: {table.num_rows:,} | "
            f"Table: {table_id}"
        )

    print(
        "All six Bronze tables loaded successfully!"
    )


# =====================================================
# 4. AIRFLOW DAG DEFINITION
# =====================================================

with DAG(

    dag_id="shopkart_end_to_end_pipeline",

    description=(
        "End-to-end ShopKart e-commerce "
        "data engineering pipeline"
    ),

    start_date=datetime(2026, 11, 18),

    schedule=None,

    catchup=False,

    max_active_runs=1,

    tags=[
        "shopkart",
        "data-engineering",
        "bigquery",
        "gcs",
    ],

) as dag:


    # ---------------------------------------------
    # START
    # ---------------------------------------------

    start = EmptyOperator(
        task_id="start_pipeline"
    )


    # ---------------------------------------------
    # GENERATE SOURCE DATA
    # ---------------------------------------------

    generate_data = EmptyOperator(
        task_id="generate_source_data"
    )


    # ---------------------------------------------
    # UPLOAD TO GCS
    # ---------------------------------------------

    upload_gcs = PythonOperator(

        task_id="upload_to_gcs",

        python_callable=upload_to_gcs,

    )


    # ---------------------------------------------
    # LOAD BRONZE
    # ---------------------------------------------

    load_bronze = PythonOperator(

        task_id="load_bronze",

        python_callable=load_bronze_tables,

    )


    # ---------------------------------------------
    # VALIDATE BRONZE
    # ---------------------------------------------

    validate_bronze = EmptyOperator(
        task_id="validate_bronze"
    )


    # ---------------------------------------------
    # TRANSFORM SILVER
    # ---------------------------------------------

    transform_silver = EmptyOperator(
        task_id="transform_silver"
    )


    # ---------------------------------------------
    # VALIDATE SILVER
    # ---------------------------------------------

    validate_silver = EmptyOperator(
        task_id="validate_silver"
    )


    # ---------------------------------------------
    # TRANSFORM GOLD
    # ---------------------------------------------

    transform_gold = EmptyOperator(
        task_id="transform_gold"
    )


    # ---------------------------------------------
    # VALIDATE GOLD
    # ---------------------------------------------

    validate_gold = EmptyOperator(
        task_id="validate_gold"
    )


    # ---------------------------------------------
    # END
    # ---------------------------------------------

    end = EmptyOperator(
        task_id="end_pipeline"
    )


    # =================================================
    # 5. TASK DEPENDENCIES
    # =================================================

    (
        start
        >> generate_data
        >> upload_gcs
        >> load_bronze
        >> validate_bronze
        >> transform_silver
        >> validate_silver
        >> transform_gold
        >> validate_gold
        >> end
    )
