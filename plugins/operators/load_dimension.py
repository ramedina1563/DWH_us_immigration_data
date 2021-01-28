from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

# The purpose of this operator is to insert data from staging tables to dimension tables
# For this, a connection to the postgres database is established and then the sql insert command is executed
class LoadDimensionOperator(BaseOperator):
    
    insert_sql = """
        INSERT INTO {}.{} (
        {}
        )
    """
    
    truncate_table = """
    TRUNCATE TABLE {}.{}
    """

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 schema,
                 table,
                 sql_query,
                 truncate,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.schema = schema
        self.table = table
        self.sql_query = sql_query
        self.truncate = truncate

    def execute(self, context):
        self.log.info("Connecting to Postgres Database on Redshift")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info(f"Loading data from staging table to dimension Table {self.table} with truncation = {self.truncate}")
        if self.truncate == True:
            truncate_sql = LoadDimensionOperator.truncate_table.format(
                self.schema,
                self.table
            )
            redshift.run(truncate_sql)
        
        final_sql = LoadDimensionOperator.insert_sql.format(
            self.schema,
            self.table,
            self.sql_query
        )
        redshift.run(final_sql)
