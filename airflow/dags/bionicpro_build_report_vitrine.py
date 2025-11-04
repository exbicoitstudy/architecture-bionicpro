from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import psycopg2
from clickhouse_driver import Client as ClickHouseClient
import os

CRM_CONN = {
    "host": os.getenv("CRM_DB_HOST", "crm_db"),
    "dbname": os.getenv("CRM_DB_NAME", "crm_db"),
    "user": os.getenv("CRM_DB_USER", "crm_user"),
    "password": os.getenv("CRM_DB_PASSWORD", "crm_password"),
    "port": int(os.getenv("CRM_DB_PORT", "5432")),
}

TELEMETRY_CONN = {
    "host": os.getenv("TELEMETRY_DB_HOST", "telemetry_db"),
    "dbname": os.getenv("TELEMETRY_DB_NAME", "telemetry_db"),
    "user": os.getenv("TELEMETRY_DB_USER", "telemetry_user"),
    "password": os.getenv("TELEMETRY_DB_PASSWORD", "telemetry_password"),
    "port": int(os.getenv("TELEMETRY_DB_PORT", "5432")),
}

CH_CONN = {
    "host": os.getenv("CLICKHOUSE_HOST", "clickhouse"),
    "port": int(os.getenv("CLICKHOUSE_PORT", "9000")),
    "user": os.getenv("CLICKHOUSE_USER", "ch_user"),
    "password": os.getenv("CLICKHOUSE_PASSWORD", "ch_password"),
    "database": os.getenv("CLICKHOUSE_DB", "reports_olap"),
}

def fetch_crm_data(**context):
    crm_conn = psycopg2.connect(**CRM_CONN)
    cur = crm_conn.cursor()
    cur.execute("""
        SELECT u.email, d.serial_number
        FROM users u
        JOIN devices d ON d.user_id = u.id;
    """)
    rows = cur.fetchall()
    crm_conn.close()

    ownership_map = {serial_number: email for (email, serial_number) in rows}
    print(f"Fetched {len(ownership_map)} CRM records")
    return ownership_map

def fetch_telemetry_data(**context):
    tele_conn = psycopg2.connect(**TELEMETRY_CONN)
    cur = tele_conn.cursor()
    cur.execute("""
        SELECT id,
               serial_number,
               reaction_latency,
               movement_accuracy,
               battery_level_percent,
               battery_temp,
               event_time
        FROM telemetry_events;
    """)
    rows = cur.fetchall()
    tele_conn.close()
    print(f"Fetched {len(rows)} telemetry rows")
    return rows

def load_to_clickhouse(**context):
    ownership_map = context['ti'].xcom_pull(task_ids='fetch_crm_data')
    telemetry_rows = context['ti'].xcom_pull(task_ids='fetch_telemetry_data')
    now_ts = datetime.utcnow()

    ch_rows = []
    for (
        event_id,
        serial_number,
        reaction_latency,
        movement_accuracy,
        battery_level_percent,
        battery_temp,
        event_time
    ) in telemetry_rows:
        user_email = ownership_map.get(serial_number, "")
        ch_rows.append((
            str(event_id),
            user_email,
            serial_number,
            event_time,
            reaction_latency,
            movement_accuracy,
            battery_level_percent,
            battery_temp,
            now_ts
        ))

    print(f"Prepared {len(ch_rows)} rows to insert into ClickHouse")

    ch = ClickHouseClient(**CH_CONN)
    ch.execute("""
        CREATE TABLE IF NOT EXISTS user_telemetry_history (
            event_id String,
            user_email String,
            serial_number String,
            event_time DateTime,
            reaction_latency Float32,
            movement_accuracy Float32,
            battery_level_percent Float32,
            battery_temp Float32,
            loaded_at DateTime
        )
        ENGINE = MergeTree()
        PARTITION BY toYYYYMMDD(event_time)
        ORDER BY (user_email, serial_number, event_id, event_time)
    """)

    existing = ch.execute("SELECT event_id FROM user_telemetry_history")
    existing_ids = {eid for (eid,) in existing}

    new_rows = [row for row in ch_rows if row[0] not in existing_ids]

    print(f"{len(new_rows)} new rows")
    if new_rows:
        ch.execute("""
            INSERT INTO user_telemetry_history (
                event_id,
                user_email,
                serial_number,
                event_time,
                reaction_latency,
                movement_accuracy,
                battery_level_percent,
                battery_temp,
                loaded_at
            )
            VALUES
        """, new_rows)
        print("INSERT done")


default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 10, 27),
    "retries": 0,
}

with DAG(
    dag_id="bionicpro_build_report_vitrine",
    default_args=default_args,
    schedule_interval="*/30 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["bionicpro", "reports", "telemetry"],
) as dag:
    fetch_crm = PythonOperator(
        task_id="fetch_crm_data",
        python_callable=fetch_crm_data,
        provide_context=True
    )

    fetch_telemetry = PythonOperator(
        task_id="fetch_telemetry_data",
        python_callable=fetch_telemetry_data,
        provide_context=True
    )

    load_clickhouse = PythonOperator(
        task_id="load_to_clickhouse",
        python_callable=load_to_clickhouse,
        provide_context=True
    )

    [fetch_crm, fetch_telemetry] >> load_clickhouse
