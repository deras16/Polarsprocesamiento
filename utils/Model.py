import polars as pl
import config.connections as conn
from tqdm import tqdm
import gc
# base class for all models ETL
class Model:
    
    # TODO check base class for all models ETL
    def __init__(self, table_name: str, id_column: str):
        self.table_name = table_name
        self.id_column = id_column
        self.postgres_connection = conn.postgresql_connection()
        self.mssql_connection = conn.msserver_connection()

    def extract_query(self) -> str:
        raise NotImplementedError("The Extract Query is not implemented. Please implement it in the subclass.")

    def extract(self) -> pl.DataFrame:     
        df = pl.DataFrame(pl.read_database_uri(query=self.extract_query(), uri=self.postgres_connection, engine='connectorx'))
        return df         

    def transform_mappings(self) -> dict:
        raise NotImplementedError("Transform mappings is not implemented. Please implement it in the subclass.")
    
    def transform(self, df: pl.DataFrame) -> pl.DataFrame:   
        mappings = self.transform_mappings() 
        for old_name, (new_name, dtype) in mappings.items():
            df = df.rename({old_name: new_name})
            df = df.with_columns(df[new_name].cast(dtype))
        return df

    #TODO check load method on performance
    def load(self, df: pl.DataFrame):
        df_load = self._check_different_rows(df)
        total_rows = df_load.shape[0]

        if total_rows > 0:
            with tqdm(total=1, desc=f"Loading {self.table_name} data") as pbar:
                df_load.write_database(table_name=self.table_name, connection=self.mssql_connection, if_table_exists="append")
                pbar.update(1)
            """ with tqdm(total=total_rows, desc=f"Loading {self.table_name} data", unit="row") as pbar:
                # Iterate over each row and process it individually
                for row in df_load.iter_rows(named=True):
                    # Here you would process and load each row into the database.

                    # Uncomment and adjust the actual insertion logic as necessary.
                    df_row = pl.DataFrame([row])
                    df_row.write_database(table_name=self.table_name, connection=self.mssql_connection, if_table_exists="append")
                    pbar.update(1) """
            tqdm.write(f"{self.table_name} Data Loading Completed.") 
        else:
            tqdm.write(f'No data to load for {self.table_name} table.')

    def _check_different_rows(self, df: pl.DataFrame) -> pl.DataFrame:
        querySQLServer = f"""
            select * from {self.table_name}
        """
        df_sql_server = pl.DataFrame(pl.read_database_uri(query=querySQLServer, uri=self.mssql_connection, engine='connectorx'))
        
        #return existing rows 
        df_result = df.join(df_sql_server, on=self.id_column, how="semi")

        #delete existing rows on df
        df_filter = df.filter(~df[self.id_column].is_in(df_result[self.id_column]))

        return df_filter
    
    def run(self):
        df = self.extract()
        df = self.transform(df)
        self.load(df)
        # Free memory by deleting the dataframe and running the garbage collector
        del df
        gc.collect()
        



