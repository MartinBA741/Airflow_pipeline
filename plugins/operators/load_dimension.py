from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 column_list=[],
                 select_sql="",
                 truncate_insert=True,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.column_list = column_list
        self.select_sql = select_sql
        self.truncate_insert=truncate_insert

    def execute(self, context):
        '''Load dimension tables'''
        self.log.info(f'Loading  Dimension {self.table}')
        redshift_hook = PostgresHook(self.redshift_conn_id)
        columns = ','.join(self.column_list)
        sql_stmt = f"insert into {self.table} ({columns}) " + self.select_sql
        if self.truncate_insert:
            redshift_hook.run(f"delete from {self.table}")
        redshift_hook.run(sql_stmt)