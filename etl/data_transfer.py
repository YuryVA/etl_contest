import datetime
from contextlib import contextmanager

import pymysql
from tests.fixtures import Container
from tests.helpers import load_assets_to_source_db, load_struct_to_destination_db


@contextmanager
def db_connector(cred):
    conn = pymysql.connect(**cred, cursorclass=pymysql.cursors.DictCursor)
    cur = conn.cursor()
    yield cur
    conn.commit()
    conn.close()


def get_latest_date(cred):
    with db_connector(cred) as cursor:
        sql = "SELECT max(dt) as date FROM transactions_denormalized"
        cursor.execute(sql)
        result = cursor.fetchone()
    return result


def get_begin_date(cred):
    with db_connector(cred) as cursor:
        sql = "SELECT min(dt) as date FROM transactions"
        cursor.execute(sql)
        result = cursor.fetchone()
    return result


def get_data_batch(cred, date_start, query):
    date_finish = date_start + datetime.timedelta(hours=1)
    with db_connector(cred) as cursor:
        cursor.execute(query, (date_start, date_finish))
        result = cursor.fetchall()
    return result


def insert_data_batch(cred, data):
    placeholders = ", ".join(["%s"] * len(data[0]))
    columns = ", ".join(data[0].keys())
    data_batch = [tuple(raw.values()) for raw in data]
    table = "transactions_denormalized"
    with db_connector(cred) as cursor:
        sql = "INSERT INTO %s ( %s ) VALUES ( %s )" % (table, columns, placeholders)
        cursor.executemany(sql, data_batch)


def data_transfer(creds_src, creds_dst):
    latest_date = get_latest_date(creds_dst)
    begin_date = get_begin_date(creds_src)
    if latest_date["date"]:
        date = latest_date["date"]
        print(f"Last entry found in destination table on {date}. Resume transfer")
    elif begin_date["date"]:
        date = begin_date["date"]
        print(f"No entry found in destination table. Start a new transfer")
    else:
        return "Nothing to transfer. The source table is empty"
    query_in = """
                SELECT t.id, t.dt, t.idoper, t.move, t.amount,
                ot.name as name_oper
                FROM transactions t
                JOIN operation_types ot ON t.idoper = ot.id
                WHERE t.dt >= %s and t.dt < %s
                """
    delta = datetime.timedelta(hours=1)
    while True:
        batch = get_data_batch(creds_src, date, query=query_in)
        if not batch:
            break
        insert_data_batch(creds_dst, batch)
        date = date + delta


if __name__ == "__main__":
    with Container() as c1, Container() as c2:
        creds_src = c1.credentials
        creds_dst = c2.credentials
        load_struct_to_destination_db(creds_dst)
        load_assets_to_source_db(creds_src)
        data_transfer(creds_src, creds_dst)
