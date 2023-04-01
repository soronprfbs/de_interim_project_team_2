import logging
import pendulum
import requests

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

log = logging.getLogger(__name__)
postgres_conn_id = 'postgresql_de'


def upload_data_to_staging(s3_filename):
    log.info(f'Start download: {s3_filename}')
    response = requests.get(s3_filename)
    local_filename = s3_filename.replace('https://storage.yandexcloud.net/hackathon/', '')
    log.info(f'Save local file: {local_filename}')
    open(f'/lessons/events/{local_filename}', 'wb').write(response.content)


@dag(
    dag_id='DE_team2_work',
    start_date=pendulum.datetime(2023, 3, 1, tz="UTC"),
    schedule_interval=None,
    tags=['download', 'stg', 'spark', 'update']
)
def download_files_dag():

    @task(task_id='fetch_s3_file')
    # загружаем файл по прямой ссылке и сохраняем
    def load_files():
        s3_filename = f'https://storage.yandexcloud.net/hackathon/events-2022-Sep-30-2134.parquet.gz'
        upload_data_to_staging(s3_filename)

    # извлекаем из архива файлы
    unzip_files = BashOperator(
        task_id='unzip_files',
        bash_command='cd /lessons/events/; gzip -d -f *.gz'
    )

    # обработаем данные в Spark и выгрузим в витрину
    load_first_dm = SparkSubmitOperator(
        task_id='load_first_dm',
        dag=dag,
        application='/scripts/first_dm.py',
        conn_id='yarn_spark',
        application_args=[
            '/lessons/events'
        ],
        conf={
            'spark.driver.maxResultSize': '20g'
        },
        executor_cores=2,
        executor_memory='2g'
    )

    # загрузим/обновим данные в итоговых витринах
    update_dm_events_by_dt = PostgresOperator(
        task_id='update_dm_events_by_dt',
        postgres_conn_id=postgres_conn_id,
        sql='sql/events_by_dt_dml.sql')

    update_dm_sales_by_dt = PostgresOperator(
        task_id='update_dm_sales_by_dt',
        postgres_conn_id=postgres_conn_id,
        sql='sql/sales_by_dt_dml.sql')

    update_dm_top_urls = PostgresOperator(
        task_id='update_dm_top_urls',
        postgres_conn_id=postgres_conn_id,
        sql='sql/top_urls_dml.sql')

    load_files = load_files()

    (
        load_files
        >> unzip_files
        >> load_first_dm
        >> [update_dm_events_by_dt, update_dm_sales_by_dt, update_dm_top_urls]
    )


_ = download_files_dag()
