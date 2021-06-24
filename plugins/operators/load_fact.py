from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 redshift_conn_id="",
                 table="",
                 column_list=[],
                 select_sql="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.column_list = column_list
        self.select_sql = select_sql

    def execute(self, context):
        self.log.info(f'Loading  Fact table {self.table}')
        redshift_hook = PostgresHook(self.redshift_conn_id)
        columns = ','.join(self.column_list)
        sql_stmt = f"insert into {self.table} ({columns}) " + self.select_sql
        redshift_hook.run(sql_stmt)