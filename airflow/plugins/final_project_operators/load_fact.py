from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql_query="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_query = sql_query

    def execute(self, context):
        # Connect to Redshift
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(f"Loading fact table '{self.table}'")

        # Execute the INSERT statement
        # Note: Fact tables are usually append-only, so we don't truncate.
        insert_statement = f"INSERT INTO {self.table} {self.sql_query}"
        redshift.run(insert_statement)

        self.log.info(f"Success: Fact table '{self.table}' loaded.")

