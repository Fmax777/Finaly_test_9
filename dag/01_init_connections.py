from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago

# Аргументы DAG по умолчанию
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 0,
}

# Создание DAG
with DAG(
    dag_id='01_init_connections',
    default_args=default_args,
    description='DAG для автоматического добавления подключений в Airflow',
    schedule_interval=None,  # Запуск вручную
    catchup=False,
) as dag:

    # Задача для начала
    start = EmptyOperator(task_id='start')

    # Задача для добавления подключений
    add_connections = BashOperator(
        task_id='add_connections',
        bash_command='airflow connections import /opt/airflow/dags/connections.json',
    )

    # Задача для завершения
    end = EmptyOperator(task_id='end')

    # Определение зависимостей
    start >> add_connections >> end