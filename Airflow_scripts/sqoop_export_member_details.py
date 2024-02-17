from airflow import DAG
from airflow.utils.dates import  days_ago
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator

dag = DAG(
    dag_id='member_details_export_one_time',
    schedule_interval=None,
    start_date=days_ago(1)
)

cmd = (
    './sqoop_export_member_details.sh'
)

trigger_task = BashOperator(
    task_id='trigger_job',
    bash_command = cmd,
    dag = dag
)

dummy = DummyOperator(
    task_id='dummy',
    dag = dag 
)

trigger_task >> dummy
