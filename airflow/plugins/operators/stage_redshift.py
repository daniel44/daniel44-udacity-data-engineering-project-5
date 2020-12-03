from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    
    template_fields = ("s3_key",)
    copy_sql = """
            COPY {}
            FROM '{}'
            ACCESS_KEY_ID '{}'
            SECRET_ACCESS_KEY '{}'
            REGION '{}'
            TIMEFORMAT as 'epochmillisecs'
            TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
            {} 'auto' 
            {}
        """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 region="",
                 file_format="JSON",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.region= region
        self.file_format = file_format
        self.aws_credentials_id = aws_credentials_id

    def execute(self, context):
        self.log.info("Starting execution")
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Truncating redshift tables")
        redshift.run("DELETE FROM {}".format(self.table))

        self.log.info("Copying data to Redshift")

        s3_path = "s3://{}".format(self.s3_bucket) + '/' + self.s3_key

        additional=""
        if self.file_format == 'CSV':
            additional = " DELIMETER ',' IGNOREHEADER 1 "

        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.region,
            self.file_format,
            additional
        )
        redshift.run(formatted_sql)

        self.log.info(f"Copied {self.table} to Redshift")
