from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql_query="",
                 mode="append", # Mode can be 'append' or 'truncate'
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_query = sql_query
        self.mode = mode

    def execute(self, context):
        # Connect to Redshift
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # Check if mode is 'truncate' to clear the table before loading
        if self.mode == "truncate":
            self.log.info(f"Mode is 'truncate'. Clearing data from dimension table '{self.table}'...")
            redshift.run(f"TRUNCATE TABLE {self.table}")

        self.log.info(f"Loading dimension table '{self.table}'")

        # Execute the INSERT statement
        insert_statement = f"INSERT INTO {self.table} {self.sql_query}"
        redshift.run(insert_statement)

        self.log.info(f"Success: Dimension table '{self.table}' loaded.")

