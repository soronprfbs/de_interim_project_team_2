import logging
import pendulum

from airflow.decorators import dag, task
from airflow.providers.postgres.operators.postgres import PostgresOperator

log = logging.getLogger(__name__)
postgres_conn_id = 'postgresql_de'


@dag(
    dag_id='DE_team2_init_schema',
    start_date=pendulum.datetime(2023, 3, 1, tz="UTC"),
    schedule_interval=None,
    tags=['init', 'sql']
)
def init_schema_dag():
    init_schema = PostgresOperator(
        task_id='init_schema',
        postgres_conn_id=postgres_conn_id,
        sql='sql/init_schema_ddl.sql')

    init_schema


_ = init_schema_dag()
