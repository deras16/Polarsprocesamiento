import os
from dotenv import load_dotenv

# Load .env file
load_dotenv(verbose=True, override=True)

def postgresql_connection() -> str:
   """
   Returns the connection string for the PostgreSQL database
   """
   return f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"

def msserver_connection() -> str:
   """
   Returns the connection string for the Microsoft SQL Server database
   """
   return f"mssql+pyodbc://{os.getenv('MSSERVER_USER')}:{os.getenv('MSSERVER_PASSWORD')}@{os.getenv('MSSERVER_HOST')}:{os.getenv('MSSERVER_PORT')}/{os.getenv('MSSERVER_DB')}?driver=ODBC+Driver+17+for+SQL+Server"
