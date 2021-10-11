import pymysql
from etl.data_transfer import data_transfer

from .helpers import ping_container


def test_container_is_alive(mysql_source_image):
    assert ping_container(mysql_source_image)


def test_containers_assets_is_ready(mysql_source_image, mysql_destination_image):

    src_conn = pymysql.connect(
        **mysql_source_image, cursorclass=pymysql.cursors.DictCursor
    )

    with src_conn:
        with src_conn.cursor() as c:
            src_query = """
                SELECT
                    COUNT(*) AS total
                FROM transactions t
                    JOIN operation_types ot ON t.idoper = ot.id
            """

            c.execute(src_query)
            src_result = c.fetchone()

    dst_conn = pymysql.connect(
        **mysql_destination_image, cursorclass=pymysql.cursors.DictCursor
    )

    with dst_conn:
        with dst_conn.cursor() as c:
            dst_query = """
                SELECT
                    COUNT(*) AS total
                FROM transactions_denormalized t
            """

            c.execute(dst_query)
            dst_result = c.fetchone()

    assert src_result["total"] > 0
    assert dst_result["total"] == 0


def test_transfer_all(mysql_source_image, mysql_destination_image):
    """

    :param mysql_source_image: Контейнер mysql-источника с исходными данными
    :param mysql_destination_image: Контейнер mysql-назначения
    :return:
    """
    data_transfer(mysql_source_image, mysql_destination_image)

    src_conn = pymysql.connect(
        **mysql_source_image, cursorclass=pymysql.cursors.DictCursor
    )

    with src_conn:
        with src_conn.cursor() as c:
            src_query = """
                    SELECT t.id, t.dt, t.idoper, t.move, t.amount,
                    ot.name as name_oper
                    FROM transactions t
                    JOIN operation_types ot ON t.idoper = ot.id
                """

            c.execute(src_query)
            src_result = c.fetchall()

    dst_conn = pymysql.connect(
        **mysql_destination_image, cursorclass=pymysql.cursors.DictCursor
    )

    with dst_conn:
        with dst_conn.cursor() as c:
            dst_query = """
                    SELECT *
                    FROM transactions_denormalized t
                """

            c.execute(dst_query)
            dst_result = c.fetchall()

    assert src_result == dst_result


def test_resume_transfer(mysql_source_image, mysql_destination_image_2):
    """

    :param mysql_source_image: Контейнер mysql-источника с исходными данными
    :param mysql_destination_image_2: Контейнер mysql-назначения
    :return:
    """

    data_transfer(mysql_source_image, mysql_destination_image_2)

    src_conn = pymysql.connect(
        **mysql_source_image, cursorclass=pymysql.cursors.DictCursor
    )

    with src_conn:
        with src_conn.cursor() as c:
            src_query = """
                    SELECT t.id, t.dt, t.idoper, t.move, t.amount,
                    ot.name as name_oper
                    FROM transactions t
                    JOIN operation_types ot ON t.idoper = ot.id
                """

            c.execute(src_query)
            src_result = c.fetchall()

    dst_conn = pymysql.connect(
        **mysql_destination_image_2, cursorclass=pymysql.cursors.DictCursor
    )

    with dst_conn:
        with dst_conn.cursor() as c:
            dst_query = """
                    SELECT *
                    FROM transactions_denormalized t
                """

            c.execute(dst_query)
            dst_result = c.fetchall()

    assert src_result == dst_result
