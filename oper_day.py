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

import sqlite3
import psycopg2
from psycopg2 import Error


from config_con_set import host, user, password, db_name

connection = psycopg2.connect(
    host=host,
    user=user,
    password=password,
    database=db_name
)

sql_set = """
    SELECT to_char(pur.datecommit, 'dd-mm-yyyy'),
    ses.shopnum as mag, ses.cashnum as kassa,
    pro.name, pro.erpcode,
    pos.qnty, pos.priceend, pos.sumfield
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


def sql_query(connection, sql_set):
    try:
        with connection.cursor() as cursor:
            cursor.execute(sql_set)
            data_for_stage = cursor.fetchall()
        return data_for_stage
            #print(*cursor.fetchall(), sep='\n')
    except Exception as _ex:
        print("[INFO] Error while working with PostgreSQL", _ex)
    finally:
        if connection:
            connection.close()
            print("[INFO] PostgeSQL connection closed")


#sql_query(connection, sql_set)
#print(sql_query(connection, sql_set))

sql_stage = """
    create table stage1 (
    date TEXT,
    mag INTEGER,
    kassa INTEGER,
    product TEXT,
    erpcode TEXT,
    qnty INTEGER,
    price INTEGER,
    sum INTEGER
    );
"""


def sqlite_try():
    try:
        sqlite_connection = sqlite3.connect('stage.db')
        cursor = sqlite_connection.cursor()
        print("База данных создана и успешно подключена к SQLite")

        cursor.execute(sql_stage)
        print("Таблица создана")
        #cursor.execute("""drop table stage1;""")
        #print(*cursor.fetchall(), sep='\n')

        cursor.close()

    except sqlite3.Error as error:
        print("Ошибка при подключении к sqlite", error)
    finally:
        if (sqlite_connection):
            sqlite_connection.close()
            print("Соединение с SQLite закрыто")


#sqlite_try()

def update_stage(param):
    try:
        sqlite_connection = sqlite3.connect('stage.db')
        cursor = sqlite_connection.cursor()
        print("База данных создана и успешно подключена к SQLite")

        sqlite_insert_query = """INSERT INTO stage1
                            (date, mag, kassa, product, erpcode, qnty, price, sum)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?);"""
        cursor.executemany(sqlite_insert_query, param)
        sqlite_connection.commit()
        print("Записи успешно вставлены в таблицу stage1", cursor.rowcount)
        sqlite_connection.commit()
        cursor.close()
    except sqlite3.Error as error:
        print("Ошибка при подключении к sqlite", error)
    finally:
        if (sqlite_connection):
            sqlite_connection.close()
            print("Соединение с SQLite закрыто")


update_stage(sql_query(connection, sql_set))