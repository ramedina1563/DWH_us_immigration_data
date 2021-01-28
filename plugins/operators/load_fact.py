from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

# The purpose of this operator is to insert data from staging tables to a fact table
# For this, a connection to the postgres database is established and then the sql insert command is executed
class LoadFactOperator(BaseOperator):
    sql_code = """
    INSERT INTO {}.{} (
        {}
    )
    """

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 schema="",
                 table="",
                 sql_query="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.schema = schema
        self.table = table
        self.sql_query = sql_query

    def execute(self, context):
        
        self.log.info("Connectring to Postgres Database on Redshift")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info(f"Loading data from staging table to Fact Table {self.table}")
        final_sql = LoadFactOperator.sql_code.format(
            self.schema,
            self.table,
            self.sql_query
        )
        redshift.run(final_sql)
