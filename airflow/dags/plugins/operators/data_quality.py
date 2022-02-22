from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table_names=[""],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table_names = table_names

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        for table in self.table_names:
            records = redshift.get_records(
                "SELECT COUNT(*) FROM {}".format(table))
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(
                    "Data quality check failed. {} returned no results".format(table))

        dq_checks = [
            {'table': 'kaggle_data.summary_korea_medal',
             'check_sql': "SELECT COUNT(*) FROM kaggle_data.summary_korea_medal WHERE sport is null",
             'expected_result': 0}
        ]
        for check in dq_checks:
            records = redshift.get_records(check['check_sql'])
            if records[0][0] != check['expected_result']:
                print("Number of rows with null ids: ", records[0][0])
                print("Expected number of rows with null ids: ",
                      check['expected_result'])
                raise ValueError(
                    "Data quality check failed. {} contains null in id column".format(check['table']))
