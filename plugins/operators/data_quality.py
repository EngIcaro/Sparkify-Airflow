from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 postgres_conn_id   = "",
                 tables_quality     = "",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.postgres_conn_id  = postgres_conn_id
        self.tables_quality = tables_quality

    def execute(self, context):
        self.log.info('[DATA QUALITY] connect to redshift')
        redshift_hook = PostgresHook("redshift")
        for i in self.tables_quality:
            result = redshift_hook.get_records("SELECT * FROM {}".format(i))
            if(len(result) < 1 or len(result[0])<1):
                self.log.error("[ERROR] Not passed in Data Quality {}".format(i))
                raise ValueError("[ERROR] Not passed in Data Quality {}".format(i))
        self.log.info('[SUCESS] Passed in Data Quality')