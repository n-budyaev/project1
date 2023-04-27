"""
Попытка создать запрос к базе set_operday и выбрать оттуда дату, номер кассы,
магазина, наименование товара, количество, цену и сумму.

Задача: эту выборку поместить в таблицу SQLite и затем работать с ней
в качестве стейжа для аналитики.

Первую часть выполнил - получил выборку с нужными полями.
План по второй части:
- создать базу SQLite и в ней таблицу с нужными полями
- передать результаты запроса из первой части задачи в новую таблицу, причем,
построчно, либо пакетно

"""


import psycopg2
from config_con_set import host, user, password, db_name

connection = psycopg2.connect(
    host=host,
    user=user,
    password=password,
    database=db_name
)

sql = """
    SELECT to_char(pur.datecommit, 'dd-mm-yyyy'),
    ses.cashnum as kassa, ses.shopnum as mag,
    pro.name, pro.erpcode,
    pos.qnty, pos.priceend, pos.sumfield, pos.excise
FROM od_session ses
join od_purchase pur
    on ses.id = pur.id_session
left join od_position pos
    on pur.id = pos.id_purchase
join od_product pro
    ON pos.product_hash = pro.hash
WHERE pur.checkstatus = 1 AND pur.operationtype is TRUE
    and ses.cashnum = 1 and ses.shopnum = 44
order by pur.id desc limit 65;
    """


def sql_query(connection, sql):
    try:
        with connection.cursor() as cursor:
            cursor.execute(sql)
            print(*cursor.fetchall(), sep='\n')
    except Exception as _ex:
        print("[INFO] Error while working with PostgreSQL", _ex)
    finally:
        if connection:
            connection.close()
            print("[INFO] PostgeSQL connection closed")


sql_query(connection, sql)
