import psycopg2
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# Replace these with your database details
class PostgresqlToParquet:
    def __init__(self, host:str, database_name:str, username:str, password:str, table_list:list) -> None:
        self.db_params = {
            "database": database_name,
            "user": username,
            "password": password,
            "host": host,
        }
        self.conn = psycopg2.connect(**self.db_params)
        self.table_list = table_list

    def sql_to_parquet(self) -> None:
        for table_name in self.table_list:
            query = f"SELECT * FROM {table_name}"
            df = pd.read_sql(query, self.conn)
            parquet_filename = f"./data/{table_name}.parquet"
            table = pa.Table.from_pandas(df)
            pq.write_table(table, parquet_filename)

        self.conn.close()


database = PostgresqlToParquet(
    database_name="ecom1692111647951egphgoeocortbeiy",
    username="deumjhxhzvgcoaunnugtbenu@psql-mock-database-cloud",
    password= "tcinixmdjluazpkqptdhnxrh",
    host= "psql-mock-database-cloud.postgres.database.azure.com",
    table_list = [
                "customers",
                "employees",
                "offices",
                "orderdetails",
                "orders",
                "payments",
                "product_lines",
                "products",
            ]
)
database.sql_to_parquet()