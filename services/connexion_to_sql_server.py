from sqlalchemy import create_engine
import urllib


def connect_with_pymssql(server, database):
    return create_engine(r'mssql+pymssql://{}/{}'.format(server, database))


def connect_with_pymssql_login(server, database, username, password):
    return create_engine(r'mssql+pymssql://{}:{}@{}/{}'.format(username, password, server, database))


def connect_with_pyodbc_driver_params(driver, server, database):
    params = urllib.parse.quote_plus(
        "DRIVER={};"
        "SERVER={};"
        "DATABASE={};".format(driver, server, database)
    )
    return create_engine("mssql+pyodbc:///?odbc_connect={}".format(params))


def connect_with_pyodbc_sql_server(server, database):
    params = urllib.parse.quote_plus(
        "DRIVER={};"
        "SERVER={};"
        "DATABASE={};".format("{SQL Server}", server, database)
    )
    return create_engine("mssql+pyodbc:///?odbc_connect={}".format(params))
