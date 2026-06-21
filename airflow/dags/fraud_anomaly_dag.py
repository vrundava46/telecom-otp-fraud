from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG("otp_fraud_anomaly", start_date=datetime(2026, 1, 1),
         schedule="@daily", catchup=False) as dag:
    BashOperator(task_id="anomaly_scoring",
                 bash_command="python -m batch.run_anomaly")
