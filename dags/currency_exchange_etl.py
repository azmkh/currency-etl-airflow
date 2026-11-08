"""
## Currency Exchange Rate ETL

Every day this DAG:

1. **Extract**   - downloads the latest exchange rates from the
   [Frankfurter API](https://frankfurter.dev/).
2. **Transform** - reshapes each rate into a clean document:
   `base_currency, target_currency, exchange_rate, rate_date, source, retrieved_at`.
3. **Load**      - saves the documents into MongoDB
   (database `currency_db`, collection `exchange_rates`).

Data moves between tasks with **named XComs** (an XCom pushed with an explicit `key`):

| From task      | XCom key            | To task        |
|----------------|---------------------|----------------|
| `extract_rates`   | `raw_exchange_rates`         | `transform_rates` |
| `transform_rates` | `transformed_exchange_rates` | `load_rates`      |
"""

from datetime import datetime, timezone

import pendulum
import requests
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.sdk import dag, task

# ---------------------------------------------------------------------------
# Settings - change these to fetch other currencies
# ---------------------------------------------------------------------------
API_URL = "https://api.frankfurter.dev/v2/rates"
BASE_CURRENCY = "EUR"
TARGET_CURRENCIES = ["USD", "GBP", "JPY", "CHF", "CAD", "AUD", "CNY"]
SOURCE_NAME = "Frankfurter API"

MONGO_CONN_ID = "mongo_currency"      # defined in .env (AIRFLOW_CONN_MONGO_CURRENCY)
MONGO_DB = "currency_db"
MONGO_COLLECTION = "exchange_rates"

# Names (keys) of our XComs - kept in one place so push and pull always match
XCOM_RAW_KEY = "raw_exchange_rates"
XCOM_TRANSFORMED_KEY = "transformed_exchange_rates"


@dag(
    dag_id="currency_exchange_etl",
    description="Daily ETL: Frankfurter API -> transform -> MongoDB",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 11, 1, tz="UTC"),
    catchup=False,               # don't create runs for past days
    doc_md=__doc__,
    default_args={
        "owner": "airflow",
        "retries": 2,
        "retry_delay": pendulum.duration(minutes=1),
    },
    tags=["etl", "currency", "mongodb"],
)
def currency_exchange_etl():

    # -----------------------------------------------------------------------
    # Task 1: EXTRACT
    # -----------------------------------------------------------------------
    @task(task_id="extract_rates")
    def extract_rates(**context):
        """Call the Frankfurter API and push the raw JSON to a named XCom."""
        params = {"base": BASE_CURRENCY, "quotes": ",".join(TARGET_CURRENCIES)}
        response = requests.get(API_URL, params=params, timeout=30)
        response.raise_for_status()          # fail the task on HTTP errors (4xx/5xx)

        raw_rates = response.json()
        # Example: [{"date": "2026-11-08", "base": "EUR", "quote": "USD", "rate": 1.121}, ...]
        if not isinstance(raw_rates, list) or not raw_rates:
            raise ValueError(f"Unexpected API response: {raw_rates!r}")

        retrieved_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
        payload = {"retrieved_at": retrieved_at, "rates": raw_rates}

        context["ti"].xcom_push(key=XCOM_RAW_KEY, value=payload)
        print(f"Extracted {len(raw_rates)} rates at {retrieved_at} (UTC)")

    # -----------------------------------------------------------------------
    # Task 2: TRANSFORM
    # -----------------------------------------------------------------------
    @task(task_id="transform_rates")
    def transform_rates(**context):
        """Pull the raw data, reshape every rate, push the result to a named XCom."""
        ti = context["ti"]
        payload = ti.xcom_pull(task_ids="extract_rates", key=XCOM_RAW_KEY)
        if not payload:
            raise ValueError(f"No XCom found with key '{XCOM_RAW_KEY}'")

        transformed = []
        for item in payload["rates"]:
            transformed.append(
                {
                    "base_currency": item["base"],
                    "target_currency": item["quote"],
                    "exchange_rate": round(float(item["rate"]), 6),
                    "rate_date": item["date"],
                    "source": SOURCE_NAME,
                    "retrieved_at": payload["retrieved_at"],
                }
            )

        ti.xcom_push(key=XCOM_TRANSFORMED_KEY, value=transformed)
        for doc in transformed:
            print(doc)
        print(f"Transformed {len(transformed)} records")

    # -----------------------------------------------------------------------
    # Task 3: LOAD
    # -----------------------------------------------------------------------
    @task(task_id="load_rates")
    def load_rates(**context):
        """Pull the transformed data and upsert it into MongoDB."""
        records = context["ti"].xcom_pull(task_ids="transform_rates", key=XCOM_TRANSFORMED_KEY)
        if not records:
            raise ValueError(f"No XCom found with key '{XCOM_TRANSFORMED_KEY}'")

        client = MongoHook(mongo_conn_id=MONGO_CONN_ID).get_conn()
        try:
            collection = client[MONGO_DB][MONGO_COLLECTION]

            # One document per (base, target, date). Re-running the DAG on the same
            # day UPDATES the rows instead of inserting duplicates (idempotent load).
            collection.create_index(
                [("base_currency", 1), ("target_currency", 1), ("rate_date", 1)],
                unique=True,
                name="uniq_base_target_date",
            )

            inserted = updated = 0
            for record in records:
                result = collection.update_one(
                    {
                        "base_currency": record["base_currency"],
                        "target_currency": record["target_currency"],
                        "rate_date": record["rate_date"],
                    },
                    {"$set": record},
                    upsert=True,
                )
                if result.upserted_id is not None:
                    inserted += 1
                else:
                    updated += 1

            total = collection.count_documents({})
            print(
                f"Loaded into {MONGO_DB}.{MONGO_COLLECTION}: "
                f"{inserted} inserted, {updated} updated, {total} documents in total"
            )
        finally:
            client.close()

    # Task order: extract -> transform -> load
    # (data travels through the named XComs, so we set the order explicitly)
    extract_rates() >> transform_rates() >> load_rates()


currency_exchange_etl()
