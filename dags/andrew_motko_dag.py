from airflow import DAG
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.utils.state import State
from airflow.utils.dates import days_ago
import random
import time
import logging

def random_medal_choice():
    """Randomly select an Olympic medal type."""
    return random.choice(["Gold", "Silver", "Bronze"])

def delay_execution():
    """Simulate processing delay."""
    logging.info("Starting delay task...")
    time.sleep(20)
    logging.info("Delay task completed.")

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "retries": 1,
}

mysql_connection_id = "mysql_connection_andrew"

with DAG(
    "andrew_motko_dag",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["olympic_medal_counting"],
) as dag:
    create_table_task = MySqlOperator(
        task_id="create_medal_table",
        mysql_conn_id=mysql_connection_id,
        sql=""" 
        CREATE TABLE IF NOT EXISTS neo_data.andrew_motko_medal_counts (
            id INT AUTO_INCREMENT PRIMARY KEY,
            medal_type VARCHAR(10),
            medal_count INT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """,
    )

    select_medal_task = PythonOperator(
        task_id="select_medal",
        python_callable=random_medal_choice,
        do_xcom_push=True,
    )

    def branching_logic(**kwargs):
        """Determine which medal counting task to run."""
        selected_medal = kwargs['ti'].xcom_pull(task_ids="select_medal")
        medal_task_map = {
            "Gold": "count_gold_medals",
            "Silver": "count_silver_medals",
            "Bronze": "count_bronze_medals"
        }
        return medal_task_map.get(selected_medal, "count_bronze_medals")

    branching_task = BranchPythonOperator(
        task_id="branch_based_on_medal",
        python_callable=branching_logic,
        provide_context=True,
    )

    count_medal_tasks = {
        "Gold": """
            INSERT INTO neo_data.andrew_motko_medal_counts (medal_type, medal_count)
            SELECT 'Gold', COUNT(*)
            FROM olympic_dataset.athlete_event_results
            WHERE medal = 'Gold';
        """,
        "Silver": """
            INSERT INTO neo_data.andrew_motko_medal_counts (medal_type, medal_count)
            SELECT 'Silver', COUNT(*)
            FROM olympic_dataset.athlete_event_results
            WHERE medal = 'Silver';
        """,
        "Bronze": """
            INSERT INTO neo_data.andrew_motko_medal_counts (medal_type, medal_count)
            SELECT 'Bronze', COUNT(*)
            FROM olympic_dataset.athlete_event_results
            WHERE medal = 'Bronze';
        """
    }

    count_bronze_task = MySqlOperator(
        task_id="count_bronze_medals",
        mysql_conn_id=mysql_connection_id,
        sql=count_medal_tasks["Bronze"],
    )

    count_silver_task = MySqlOperator(
        task_id="count_silver_medals",
        mysql_conn_id=mysql_connection_id,
        sql=count_medal_tasks["Silver"],
    )

    count_gold_task = MySqlOperator(
        task_id="count_gold_medals",
        mysql_conn_id=mysql_connection_id,
        sql=count_medal_tasks["Gold"],
    )

    delay_task = PythonOperator(
        task_id="delay_task",
        python_callable=delay_execution,
        trigger_rule=TriggerRule.ONE_SUCCESS,
    )

    check_last_record_task = SqlSensor(
        task_id="verify_recent_record",
        conn_id=mysql_connection_id,
        sql="""
            WITH count_in_medals AS (
                SELECT COUNT(*) as nrows 
                FROM neo_data.andrew_motko_medal_counts
                WHERE created_at >= NOW() - INTERVAL 30 SECOND
            )
            SELECT nrows > 0 AS result FROM count_in_medals;  -- Покращене порівняння, щоб повернути булеве значення
        """,
        mode="poke",
        poke_interval=10,
        timeout=30,  
    )

    create_table_task >> select_medal_task >> branching_task
    branching_task >> [count_bronze_task, count_silver_task, count_gold_task]
    [count_bronze_task, count_silver_task, count_gold_task] >> delay_task
    delay_task >> check_last_record_task