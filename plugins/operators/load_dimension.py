from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 postgres_conn_id   = "",
                 dim_table          = "",
                 sql_query          = "",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.postgres_conn_id  = postgres_conn_id
        self.dim_table         = dim_table
        self.sql_query         = sql_query

    def execute(self, context):
        self.log.info("[LOAD_DIM] connect to redshift")
        # Conectar com o Hook da redshift
        redshift_hook = PostgresHook("redshift")
        self.log.info("[LOAD_FACT] Load {} from staging tables to fact table".format(self.dim_table))
        # fazer a leitura da tabela staging e levar para a tabela fato
        redshift_hook.run("INSERT INTO {} {}".format(self.dim_table, self.sql_query))