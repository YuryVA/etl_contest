import datetime
from contextlib import contextmanager

import pymysql
from tests.fixtures import Container
from tests.helpers import (load_assets_to_destination_db,
                           load_assets_to_source_db,
                           load_struct_to_destination_db,
                           load_struct_to_source_db)

src_table_name_main = "transactions"
src_table_name_sec = "operation_types"
dst_table_name = "transactions_denormalized"

resume_query = """
               SELECT t.id, t.dt, t.idoper, t.move, t.amount,
               ot.name as name_oper
               FROM transactions t
               JOIN operation_types ot ON t.idoper = ot.id
               WHERE t.dt > %s and t.dt <= %s
               """
new_query = """
            SELECT t.id, t.dt, t.idoper, t.move, t.amount,
            ot.name as name_oper
            FROM transactions t
            JOIN operation_types ot ON t.idoper = ot.id
            WHERE t.dt >= %s and t.dt < %s
            """


@contextmanager
def db_connector(cred: dict) -> pymysql.connect.cursor:
    """
    Database connection management function

    Establishes a connection with sql database. Yield cursor object.
    Closes the connection upon completion of the transaction.

    :param cred: connection credentials
    :return: cursor object
    """
    conn = pymysql.connect(**cred, cursorclass=pymysql.cursors.DictCursor)
    cur = conn.cursor()
    yield cur
    conn.commit()
    conn.close()


def get_latest_date(cred: dict, table_name: str, column: str = "dt") -> dict:
    """Gets the date of the last record from the given table

    :param cred: connection credentials
    :param table_name: table name
    :param column: column name
    :return: the date of the last record
    """
    with db_connector(cred) as cursor:
        sql = "SELECT max( %s ) as date FROM %s" % (column, table_name)
        cursor.execute(sql)
        result = cursor.fetchone()
    return result


def get_begin_date(cred: dict, table_name: str, column: str = "dt") -> dict:
    """Gets the date of the first record from the given table

    :param cred: connection credentials
    :param table_name: table name
    :param column: column name
    :return: the date of the first record
    """
    with db_connector(cred) as cursor:
        sql = "SELECT min( %s ) as date FROM %s" % (column, table_name)
        cursor.execute(sql)
        result = cursor.fetchone()
    return result


def get_data_batch(cred: dict,
                   date_start: datetime.datetime,
                   query: str
                   ) -> dict:
    """Gets a batch of data at one hour intervals

    :param cred: connection credentials
    :param date_start: interval start date
    :param query: query to get data
    :return: the batch of the data
    """
    date_finish = date_start + datetime.timedelta(hours=1)
    with db_connector(cred) as cursor:
        cursor.execute(query, (date_start, date_finish))
        result = cursor.fetchall()
    return result


def insert_data_batch(cred: dict, table_name: str, data: dict):
    """Puts a batch of data into a table

    :param cred: connection credentials
    :param table_name: table name
    :param data: the batch of the data
    """
    placeholders = ", ".join(["%s"] * len(data[0]))
    columns = ", ".join(data[0].keys())
    data_batch = [tuple(raw.values()) for raw in data]
    with db_connector(cred) as cursor:
        sql = "INSERT INTO %s ( %s ) VALUES ( %s )" % (
            table_name,
            columns,
            placeholders,
        )
        cursor.executemany(sql, data_batch)


def data_transfer(creds_src: dict, creds_dst: dict):
    """Transfers data from source tables to destination

    Pipeline implements two algorithms:
    - starts a new transfer if there are no records in the destination table.
    - resume transfer from the latest date record in the destination table.

    :param creds_src: source table connection credentials
    :param creds_dst: destination table connection credentials
    """
    latest_date = get_latest_date(creds_dst, dst_table_name)
    begin_date = get_begin_date(creds_src, src_table_name_main)
    if latest_date["date"]:
        date = latest_date["date"]
        print(
            f"""Last entry found in destination table on {date}. 
        Resume transfer"""
        )
        query = resume_query
    elif begin_date["date"]:
        date = begin_date["date"]
        print(f"No entry found in destination table. Begins a new transfer")
        query = new_query
    else:
        return "Nothing to transfer. The source table is empty"
    delta = datetime.timedelta(hours=1)
    while True:
        batch = get_data_batch(creds_src, date, query=query)
        if not batch:
            break
        insert_data_batch(creds_dst, dst_table_name, batch)
        date = date + delta


if __name__ == "__main__":
    with Container() as c1, Container() as c2:
        creds_src = c1.credentials
        creds_dst = c2.credentials
        load_struct_to_destination_db(creds_dst)
        # load_assets_to_destination_db(creds_dst)
        # load_struct_to_source_db(creds_src)
        load_assets_to_source_db(creds_src)
        data_transfer(creds_src, creds_dst)
