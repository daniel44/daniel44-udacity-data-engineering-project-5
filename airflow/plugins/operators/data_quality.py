from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 aws_credentials_id="",
                 redshift_conn_id="",
                 dq_checks=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.aws_credentials_id = aws_credentials_id,
        self.redshift_conn_id = redshift_conn_id,
        self.dq_checks = dq_checks

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        for dqc in self.dq_checks:
            records = redshift_hook.get_records(dqc['check_sql'])
            if len(records) != dqc['expected_result']:
                self.log.error(f"Check failed. Test {dqc} not passed")
                raise ValueError(f"Check failed. Test {dqc} not passed")
            self.log.info(f"Check {dqc} passed.")
