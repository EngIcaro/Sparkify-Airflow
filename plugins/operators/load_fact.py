from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 postgres_conn_id   = "",
                 fact_table         = "",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.postgres_conn_id  = postgres_conn_id
        self.fact_table        = fact_table

    def execute(self, context):
        self.log.info("[LOAD_FACT] connect to redshift")
        # Conectar com o Hook da redshift
        redshift_hook = PostgresHook("redshift")
        self.log.info("[LOAD_FACT] Load song/plays from staging tables to fact table")
        # fazer a leitura da tabela staging e levar para a tabela fato
        redshift_hook.run("INSERT INTO {} {}".format(self.fact_table, SqlQueries.songplay_table_insert))