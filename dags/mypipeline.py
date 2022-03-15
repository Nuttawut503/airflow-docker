from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from datetime import date, timedelta

dag = DAG(
  'mycustompipeline',
  catchup=False,
  description='Example for data pipelining',
  schedule_interval='*/1 * * * *',
  default_args={
    'owner': 'Nuttawut',
    'start_date': days_ago(1),
    'depends_on_past': False,
    'email': ['nuttawut@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
  })

def print_message():
  print('Hello Task 1!')

def write_to_file():
  with open('/home/airflow/data/logtime.txt', 'a') as f:
    f.write(date.today().strftime('%B %d, %Y\n'))

t1 = PythonOperator(
  task_id='print_message',
  python_callable=print_message,
  dag=dag
)

t2 = PythonOperator(
  task_id='write_file',
  python_callable=write_to_file,
  dag=dag
)

t3 = BashOperator(
  task_id='print_file',
  bash_command='cat /home/airflow/data/logtime.txt',
  dag=dag
)

[t1, t2] >> t3
