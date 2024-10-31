import datetime as dt
import os
import random
import string

from airflow import DAG
from airflow.operators.python import PythonOperator

args = {'owner': 'airflow',
        'start_date': dt.datetime(2020, 2, 11),
        'retries': 1,
        'retry_delay': dt.timedelta(minutes=1),
        'depends_on_past': False,
        }

def create_text_files():
    folder_name = 'texts'

    os.makedirs(folder_name, exist_ok=True)

    for i in range(100):
        random_string = ''.join(random.choices(string.ascii_letters, k=10))
        
        file_name = f'{folder_name}/file_{i + 1}.txt'
        
        with open(file_name, 'w') as f:
            f.write(random_string)

def count_a(**context):
    num = context['params']['num']
    file_name = f'texts/file_{num + 1}.txt'
    try:
        with open(file_name, 'r', encoding='utf-8') as file:
            content = file.read()
            count_a = content.lower().count('a')

            folder_name = 'results'

            os.makedirs(folder_name, exist_ok=True)

            result_name = f'{folder_name}/{num + 1}.res'
            with open(result_name, 'w') as f:
                f.write(str(count_a))
    except:
        pass

def count_total():
    total_count = 0
    for i in range(100):
        file_name = f'results/{i + 1}.res'
        with open(file_name, 'r', encoding='utf-8') as file:
            content = file.read()
            try:
                total_count += int(content)
            except:
                pass
    with open('results/total_a_count.res', 'w') as f:
        f.write(str(total_count))

with DAG(dag_id='a_counter', default_args=args, schedule_interval=None) as dag:
    create_text_files_task = PythonOperator(task_id='create_text_files',
                                    python_callable=create_text_files,
                                    dag=dag)
    count_total_task = PythonOperator(task_id='count_total',
                                    python_callable=count_total,
                                    dag=dag)
    for i in range(100):
        count_a_task = PythonOperator(task_id=f'count_a_{i}',
                                    python_callable=count_a,
                                    params={'num': i},
                                    dag=dag)
        create_text_files_task >> count_a_task >> count_total_task
