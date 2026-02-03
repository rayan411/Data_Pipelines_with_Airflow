from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    
    # 1. Enable templating for this field to allow dynamic date-based substitution
    template_fields = ("s3_key",)
    
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION 'us-east-1'
        FORMAT AS JSON '{}'
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 json_path="auto",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_path = json_path

    def execute(self, context):
        self.log.info('StageToRedshiftOperator: Starting execution')
        
        # 2. Retrieve AWS credentials
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        
        # 3. Connect to Redshift
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        # 4. Clear the target table first (Idempotency)
        self.log.info("Clearing data from destination Redshift table")
        redshift.run("DELETE FROM {}".format(self.table))
        
        # 5. Prepare S3 path
        self.log.info("Copying data from S3 to Redshift")
        # Note: s3_key will be automatically rendered by Airflow before this step
        s3_path = "s3://{}/{}".format(self.s3_bucket, self.s3_key)
        
        # 6. Format the COPY command
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.json_path
        )
        
        # 7. Execute the COPY command
        redshift.run(formatted_sql)
        self.log.info(f"StageToRedshiftOperator: Successfully copied data to {self.table}")
