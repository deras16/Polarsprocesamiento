import os
from dotenv import load_dotenv

# Load .env file
load_dotenv()

def postgresql_connection():
   return f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"


#TODO check if the connection string is correct
def msserver_connection():
   return f"mssql+pyodbc://{os.getenv('MSSERVER_USER')}:{os.getenv('MSSERVER_PASSWORD')}@{os.getenv('MSSERVER_HOST')}:{os.getenv('MSSERVER_PORT')}/{os.getenv('MSSERVER_DB')}?driver=ODBC+Driver+17+for+SQL+Server"
