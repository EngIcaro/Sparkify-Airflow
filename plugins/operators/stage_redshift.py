from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook
from helpers.sql_queries import SqlQueries

# Load any JSON formatted files from S3 to Amazon Redshift
#                 ]Comando sql a ser executado[,
#                 Onde o s3 está localizado,
#                 Qual a tabela final,
#                 Qual o json que vai ser lido,
#                 Timestamp para o backfilling
class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    copy_sql = """
                COPY {}
                FROM '{}'
                ACCESS_KEY_ID '{}'
                SECRET_ACCESS_KEY '{}'
                {}
                """

    @apply_defaults
    def __init__(self,
                 postgres_conn_id = "",
                 aws_credentials_id="",
                 s3_bucket        = "",
                 s3_json_data     = "",
                 table_output     = "",
                 delimiter        =",", 
                 json_path         ="auto",
                 ignore_headers   = 1 ,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.postgres_conn_id   = postgres_conn_id
        self.s3_bucket          = s3_bucket
        self.s3_json_data       = s3_json_data
        self.table_output       = table_output
        self.aws_credentials_id = aws_credentials_id
        self.delimiter          = delimiter
        self.ignore_headers     = ignore_headers
        self.json_path          = json_path

    def execute(self, context):
        # Acessar o S3 bucket através do hooker usando a credencial que criou no airflow
        aws_hook = AwsHook(self.aws_credentials_id, client_type=self.postgres_conn_id)
        # Pegar minhas credenciais (key_id e access_key)
        credentials = aws_hook.get_credentials()

        # Preciso ter acesso ao meu Redshift do airflow
        redshift_hook = PostgresHook(self.postgres_conn_id)
        
        # Deletar caso já tenha a tabela de destino 
        self.log.info("Clearing data from destination Redshift {}".format(self.table_output))
        redshift_hook.run("DELETE FROM {}".format(self.table_output))
        
        # Copiar os dados do s3_json_Data e jogar para a table_output
        self.log.info("Copying data from S3 to Redshift")
        json_data_context = self.s3_json_data.format(**context) 
        s3_path = "s3://{}/{}".format(self.s3_bucket,json_data_context)
        
        file_processing = "JSON '{}'".format(self.json_path)
        
        redshift_hook.run(StageToRedshiftOperator.copy_sql.format(self.table_output,
                                                            s3_path,
                                                            credentials.access_key,
                                                            credentials.secret_key,
                                                            file_processing 
                                                             ))
        





