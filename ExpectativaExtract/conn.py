from decouple import config

def postgreConn():
    return f"postgresql://{config('POSTGRES_USER')}:{config('POSTGRES_PASS')}@{config('POSTGRES_HOST')}:{config('POSTGRES_PORT')}/{config('POSTGRES_DB')}"

def MssqlConn():
    return f"mssql+pyodbc://{config('MS_USER')}:{config('MS_PASS')}@{config('MS_HOST')}:{config('MS_PORT')}/{config('MS_DB')}"

    