from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql_query="",  # 🧠 Modification: we receive SQL statement as a parameter
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_query = sql_query

    def execute(self, context):
        self.log.info(f"LoadFactOperator: Loading fact table '{self.table}'")
        
        # 1. Create connection to Redshift
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # 2. Build INSERT statement
        # The idea here is to combine the target table name with the SELECT statement passed from the DAG
        insert_statement = f"INSERT INTO {self.table} {self.sql_query}"
        
        # 3. Execute the command
        redshift.run(insert_statement)
        
        self.log.info(f"LoadFactOperator: Successfully loaded fact table '{self.table}'")