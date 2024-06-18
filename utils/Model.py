import polars as pl
import config.connections as conn
from tqdm import tqdm
import gc

class Model:
    """
    Base class for all models ETL
    """
    def __init__(self, table_name: str, id_column: str):
        """ 
        Constructor for the Model class
        Args:
            table_name (str): The name of the table to be loaded
            id_column (str): The name of the id column in the table to be loaded
        """
        self.table_name = table_name
        self.id_column = id_column
        self.postgres_connection = conn.postgresql_connection()
        self.mssql_connection = conn.msserver_connection()

    def extract_query(self) -> str:
        """ 
        Extract Query to be implemented in the subclass
        Returns:
            str: The extract query to be executed
        """
        raise NotImplementedError("The Extract Query is not implemented. Please implement it in the subclass.")

    def extract(self) -> pl.DataFrame:
        """
        Extract data from the source
        Returns:
            pl.DataFrame: The extracted data
        """     
        df = pl.DataFrame(pl.read_database_uri(query=self.extract_query(), uri=self.postgres_connection, engine='connectorx'))
        return df         

    def transform_mappings(self) -> dict:
        """
        Transform mappings to be implemented in the subclass
        Returns:
            dict: The mappings to be used in the transform method
        """
        raise NotImplementedError("Transform mappings is not implemented. Please implement it in the subclass.")
    
    def transform(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Transform the data
        Args:
            df (pl.DataFrame): The data to be transformed
        Returns:
            pl.DataFrame: The transformed data
        """  
        mappings = self.transform_mappings() 
        for old_name, (new_name, dtype) in mappings.items():
            df = df.rename({old_name: new_name})
            df = df.with_columns(df[new_name].cast(dtype))
        return df

    def load(self, df: pl.DataFrame):
        """
        Load the data into the destination
        Args:
            df (pl.DataFrame): The data to be loaded
        """
        df_load = self.check_different_rows(df)
        total_rows = df_load.shape[0]

        if total_rows > 0:
            with tqdm(total=1, desc=f"Loading {self.table_name} data") as pbar:
                df_load.write_database(table_name=self.table_name, connection=self.mssql_connection, if_table_exists="append")
                pbar.update(1)
            tqdm.write(f"{self.table_name} Data Loading Completed.") 
        else:
            tqdm.write(f'No data to load for {self.table_name} table.')

    def check_different_rows(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Check if there are different rows between the source and destination
        Args:
            df (pl.DataFrame): The data to be checked
        Returns:
            pl.DataFrame: The different rows
        """
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
        """
        Run the ETL process
        """
        df = self.extract()
        df = self.transform(df)
        self.load(df)
        # Free memory by deleting the dataframe and running the garbage collector
        del df
        gc.collect()
        



