from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                # Define your operators params (with defaults) here
                tables_list=[],
                redshift_conn_id="",
                *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.redshift_conn_id = redshift_conn_id
        self.tables_list = tables_list

    

def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)      
        for table in self.table_names:
            records = redshift.get_records(f"Select count(*) from {table}")
            self.log.info('DataQualityOperator checking for row count')
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {table} retuned no results")
        
        # list of dicts for check
        dq_checks=[
            {'table': 'users',
             'check_sql': "SELECT COUNT(*) FROM users WHERE userid is null",
             'expected_result': 0},
            {'table': 'songs',
             'check_sql': "SELECT COUNT(*) FROM songs WHERE songid is null",
             'expected_result': 0}
        ]
        for check in dq_checks:
            self.log.info('DataQualityOperator checking for Null ids')
            records = redshift.get_records(check['check_sql'])[0]
            if records[0] != check['expected_result']:
                raise ValueError(f"Data quality check failed. {check['table']} contains null in id column, got {records[0]} instead")