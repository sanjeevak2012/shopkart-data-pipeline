
import json
import subprocess
import sys
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
# 2. GENERATE SOURCE DATA
# =====================================================

def generate_source_data():

    generator_script = (
        PROJECT_ROOT
        / "scripts"
        / "generators"
        / "generate_all.py"
    )

    if not generator_script.exists():
        raise FileNotFoundError(
            f"Generator script not found: {generator_script}"
        )

    print("Starting ShopKart source data generation")

    subprocess.run(
        [
            sys.executable,
            str(generator_script)
        ],
        cwd=PROJECT_ROOT,
        check=True
    )

    print("All ShopKart source datasets generated successfully!")


# =====================================================
# 3. GENERATE UNIQUE GCS OBJECT PATH
# =====================================================

def get_gcs_object_path(dataset, context):

    ingestion_date = context["ds"]

    run_id = context["run_id"]

    # Make run_id safe for use in a GCS object path.
    safe_run_id = (
        run_id
        .replace(":", "_")
        .replace("+", "_")
        .replace("/", "_")
    )

    destination = (
        f"raw/{dataset}/"
        f"ingestion_date={ingestion_date}/"
        f"run_id={safe_run_id}/"
        f"{dataset}.csv"
    )

    return destination


# =====================================================
# 4. UPLOAD SOURCE DATA TO GCS
# =====================================================

def upload_to_gcs(**context):

    from google.cloud import storage

    credentials, _ = google.auth.default(
        scopes=[
            "https://www.googleapis.com/auth/cloud-platform"
        ]
    )

    client = storage.Client(
        project=GCP_PROJECT,
        credentials=credentials
    )

    bucket = client.bucket(GCS_BUCKET)

    for dataset in DATASETS:

        print(f"Starting GCS upload: {dataset}")

        local_file = (
            PROJECT_ROOT
            / "data"
            / "raw"
            / f"{dataset}.csv"
        )

        if not local_file.exists():
            raise FileNotFoundError(
                f"File not found: {local_file}"
            )

        # Use a unique path for every DAG run.
        destination = get_gcs_object_path(
            dataset,
            context
        )

        blob = bucket.blob(destination)

        # Prevent accidental overwriting.
        blob.upload_from_filename(
            str(local_file),
            content_type="text/csv",
            if_generation_match=0
        )

        blob.reload()

        print(
            f"SUCCESS: {dataset} | "
            f"Size: {blob.size:,} bytes | "
            f"Object: {destination}"
        )

    print("All six datasets uploaded successfully!")


# =====================================================
# 5. LOAD DATA FROM GCS INTO BIGQUERY BRONZE
# =====================================================

def load_bronze_tables(**context):

    from google.cloud import bigquery, storage

    credentials, _ = google.auth.default(
        scopes=[
            "https://www.googleapis.com/auth/cloud-platform"
        ]
    )

    bq_client = bigquery.Client(
        project=GCP_PROJECT,
        location=BQ_LOCATION,
        credentials=credentials
    )

    storage_client = storage.Client(
        project=GCP_PROJECT,
        credentials=credentials
    )

    bucket = storage_client.bucket(GCS_BUCKET)

    for dataset in DATASETS:

        print(f"Starting Bronze load: {dataset}")

        # Read from the exact GCS path created by this DAG run.
        object_name = get_gcs_object_path(
            dataset,
            context
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

        # Load the explicit BigQuery schema.
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

        fields = (
            schema_data["fields"]
            if isinstance(schema_data, dict)
            else schema_data
        )

        schema = [
            bigquery.SchemaField(
                field["name"],
                field["type"],
                mode=field.get("mode", "NULLABLE")
            )
            for field in fields
        ]

        table_id = (
            f"{GCP_PROJECT}."
            f"{BRONZE_DATASET}."
            f"{dataset}"
        )

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            schema=schema,
            autodetect=False,
            write_disposition=(
                bigquery.WriteDisposition.WRITE_TRUNCATE
            ),
            allow_quoted_newlines=True
        )

        load_job = bq_client.load_table_from_uri(
            source_uri,
            table_id,
            job_config=job_config,
            location=BQ_LOCATION
        )

        load_job.result()

        table = bq_client.get_table(table_id)

        print(
            f"SUCCESS: {dataset} | "
            f"Rows: {table.num_rows:,} | "
            f"Table: {table_id}"
        )

    print("All six Bronze tables loaded successfully!")


# =====================================================
# 6. TRANSFORM BRONZE INTO SILVER
# =====================================================

def transform_silver():

    from google.cloud import bigquery

    credentials, _ = google.auth.default(
        scopes=[
            "https://www.googleapis.com/auth/cloud-platform"
        ]
    )

    bq_client = bigquery.Client(
        project=GCP_PROJECT,
        location=BQ_LOCATION,
        credentials=credentials
    )

    sql_directory = (
        PROJECT_ROOT
        / "sql"
        / "bronze_to_silver"
    )

    sql_files = [
        "customers.sql",
        "products.sql",
        "orders.sql",
        "order_items.sql",
        "payments.sql",
        "inventory_events.sql",
    ]

    for sql_file in sql_files:

        sql_path = sql_directory / sql_file

        if not sql_path.exists():
            raise FileNotFoundError(
                f"SQL file not found: {sql_path}"
            )

        print(
            f"Starting Silver transformation: {sql_file}"
        )

        query = sql_path.read_text(
            encoding="utf-8"
        )

        query_job = bq_client.query(
            query,
            location=BQ_LOCATION
        )

        query_job.result()

        print(
            f"SUCCESS: Silver transformation completed: "
            f"{sql_file}"
        )

    print("All six Silver transformations completed!")


# =====================================================
# 7. TRANSFORM SILVER INTO GOLD
# =====================================================

def transform_gold():

    from google.cloud import bigquery

    credentials, _ = google.auth.default(
        scopes=[
            "https://www.googleapis.com/auth/cloud-platform"
        ]
    )

    bq_client = bigquery.Client(
        project=GCP_PROJECT,
        location=BQ_LOCATION,
        credentials=credentials
    )

    sql_directory = (
        PROJECT_ROOT
        / "sql"
        / "silver_to_gold"
    )

    sql_files = [
        "daily_sales_summary.sql",
        "product_performance.sql",
        "customer_analytics.sql",
        "payment_analytics.sql",
        "inventory_analytics.sql",
    ]

    for sql_file in sql_files:

        sql_path = sql_directory / sql_file

        if not sql_path.exists():
            raise FileNotFoundError(
                f"SQL file not found: {sql_path}"
            )

        print(
            f"Starting Gold transformation: {sql_file}"
        )

        query = sql_path.read_text(
            encoding="utf-8"
        )

        query_job = bq_client.query(
            query,
            location=BQ_LOCATION
        )

        query_job.result()

        print(
            f"SUCCESS: Gold transformation completed: "
            f"{sql_file}"
        )

    print("All five Gold transformations completed!")


# =====================================================
# 8. RUN DATA QUALITY VALIDATION SCRIPTS
# =====================================================

def run_validation_script(script_name):

    script_path = (
        PROJECT_ROOT
        / "scripts"
        / "validation"
        / script_name
    )

    if not script_path.exists():
        raise FileNotFoundError(
            f"Validation script not found: {script_path}"
        )

    print(f"Starting validation: {script_name}")

    subprocess.run(
        [
            sys.executable,
            str(script_path)
        ],
        cwd=PROJECT_ROOT,
        check=True
    )

    print(
        f"SUCCESS: {script_name} completed!"
    )


# =====================================================
# 9. AIRFLOW DAG DEFINITION
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

    generate_data = PythonOperator(
        task_id="generate_source_data",
        python_callable=generate_source_data
    )

    # ---------------------------------------------
    # UPLOAD TO GCS
    # ---------------------------------------------

    upload_gcs = PythonOperator(
        task_id="upload_to_gcs",
        python_callable=upload_to_gcs
    )

    # ---------------------------------------------
    # LOAD BRONZE
    # ---------------------------------------------

    load_bronze = PythonOperator(
        task_id="load_bronze",
        python_callable=load_bronze_tables
    )

    # ---------------------------------------------
    # VALIDATE BRONZE
    # ---------------------------------------------

    validate_bronze = PythonOperator(
        task_id="validate_bronze",
        python_callable=run_validation_script,
        op_kwargs={
            "script_name": "validate_bronze.py"
        }
    )

    # ---------------------------------------------
    # TRANSFORM SILVER
    # ---------------------------------------------

    silver_transformation = PythonOperator(
        task_id="transform_silver",
        python_callable=transform_silver
    )

    # ---------------------------------------------
    # VALIDATE SILVER
    # ---------------------------------------------

    validate_silver = PythonOperator(
        task_id="validate_silver",
        python_callable=run_validation_script,
        op_kwargs={
            "script_name": "validate_silver.py"
        }
    )

    # ---------------------------------------------
    # TRANSFORM GOLD
    # ---------------------------------------------

    gold_transformation = PythonOperator(
        task_id="transform_gold",
        python_callable=transform_gold
    )

    # ---------------------------------------------
    # VALIDATE GOLD
    # ---------------------------------------------

    validate_gold = PythonOperator(
        task_id="validate_gold",
        python_callable=run_validation_script,
        op_kwargs={
            "script_name": "validate_gold.py"
        }
    )

    # ---------------------------------------------
    # END
    # ---------------------------------------------

    end = EmptyOperator(
        task_id="end_pipeline"
    )

    # =================================================
    # 10. TASK DEPENDENCIES
    # =================================================

    (
        start
        >> generate_data
        >> upload_gcs
        >> load_bronze
        >> validate_bronze
        >> silver_transformation
        >> validate_silver
        >> gold_transformation
        >> validate_gold
        >> end
    )
