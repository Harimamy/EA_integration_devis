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


# if __name__ == '__main__':
    # import xlwt
    # from openpyxl import Workbook, load_workbook
    # # from xlwt import Workbook
    #
    # # wb = load_workbook('filename.xlsx')
    # wb = Workbook(write_only=True)
    # sheet1 = wb.add_sheet('Sheet 1')
    #
    # with open("""D:\sage donnees\ALU\Devis 2\MAMY01.rtf""", 'r', encoding="latin", errors='ignore') as file:
    #     for i, line in enumerate(file.read()):
    #         sheet1.write(i, 0, line)

    # add_sheet is used to create sheet.
    # sheet1 = wb.add_sheet('Sheet 1')

    # sheet1.write(0, 0, text[:32767])
    # sheet1.write(1, 0, 'ISBT DEHRADUN')
    # sheet1.write(2, 0, 'SHASTRADHARA')
    # sheet1.write(3, 0, 'CLEMEN TOWN')
    # sheet1.write(4, 0, 'RAJPUR ROAD')
    # sheet1.write(5, 0, 'CLOCK TOWER')
    # sheet1.write(0, 1, 'ISBT DEHRADUN')
    # sheet1.write(0, 2, 'SHASTRADHARA')
    # sheet1.write(0, 3, 'CLEMEN TOWN')
    # sheet1.write(0, 4, 'RAJPUR ROAD')
    # sheet1.write(0, 5, 'CLOCK TOWER')

    # wb.save('xlwt example.xlsx')
    # pass