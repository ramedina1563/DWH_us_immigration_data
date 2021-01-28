from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

# The purpose of this operator is to perform a data quality check to guarantee the correct implementation of the pipeline
# For this, first of all a connection to the postgres database is established
# Second of all, a for loop goes through the passed sql queries and compares them to the expected outcome which are also passed together with the queries
# In case the quality check is not passed, an error is raised, otherwise a success message is logged
class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 sql,
                 operation,
                 outcome,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql = sql
        self.operation = operation
        self.outcome = outcome


    def execute(self, context):
        self.log.info("Connecting to Postgres Database on Redshift")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info("Running quality checks input as sql queries and comparing them to the also input outcome")
        for x in range(0, len(self.sql)):
            
            result = redshift.get_records(self.sql[x])

            self.log.info(f'The result was {result[0][0]}')
            
            if self.operation == "equal":

                if result[0][0] != self.outcome[x]:
                    raise ValueError(f'Data quality check failed. The outcome of the sql query was not equal to {self.outcome[x]}')
                self.log.info(f'Data quality check for query {x} passed!')
            else:
                if result[0][0] > self.outcome[x]:
                    raise ValueError(f'Data quality check failed. The outcome of the sql query was not equal to {self.outcome[x]}')
                self.log.info(f'Data quality check for query {x} passed!')
                

                