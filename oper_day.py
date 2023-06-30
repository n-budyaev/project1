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

#  текст запроса из рабочей базы
# sql_set = """
#     SELECT to_char(pur.datecommit, 'dd-mm-yyyy'),
#         ses.shopnum as mag, ses.cashnum as kassa,
#         pro.name, pro.erpcode,
#         pos.qnty, pos.priceend, pos.sumfield
#     FROM od_session ses
#         join od_purchase pur
#             on ses.id = pur.id_session
#         left join od_position pos
#             on pur.id = pos.id_purchase
#         join od_product pro
#             on pos.product_hash = pro.hash
#     WHERE pur.checkstatus = 0 AND pur.operationtype is TRUE
#         and ses.cashnum = 1 and ses.shopnum = 44
#     ORDER BY pur.id desc limit 1000;
#     """
# sql_set = """
#     SELECT to_char(pur.datecommit, 'dd-mm-yyyy'),
#         ses.shopnum as mag, ses.cashnum as kassa,
#         pro.name, pro.erpcode,
#         pos.qnty, pos.priceend, pos.sumfield
#     FROM od_session ses
#         join od_purchase pur
#             on ses.id = pur.id_session
#         left join od_position pos
#             on pur.id = pos.id_purchase
#         join od_product pro
#             on pos.product_hash = pro.hash
#     WHERE pur.checkstatus = 0 AND pur.operationtype is TRUE
#         and ses.cashnum = 1 and ses.shopnum = 44
#     ORDER BY pur.id desc limit 1000;
#     """
sql_set = """
    SELECT to_char(dateoperday, 'dd-mm-yyyy') as date, shopnumber as magaz, cashnumber as kassa,
        amountbycashpurchase as nal, amountbycashlesspurchase as beznal, amountcashdiscount as skidka,
        countcashpurchase as num_chek_nal, countcashlesspurchase as num_chek_beznal,
        amountbycashreturn as vozvrat_nal, amountbycashlessreturn as vozvrat_beznal
    FROM erpi_zreport z
    --WHERE shopnumber = 50
    --ORDER BY dateoperday desc limit 1000;
    """







#  выполняю запрос из рабочей базы
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
# print(sql_query(connection, sql_set))

# сначала удаляю старые данные вместе с таблицей
sql_drop_table = """DROP TABLE IF EXISTS stage1;"""

# создаю новую таблицу с нужными полями
sql_stage = """
    create table stage1 (
    date TEXT,
    mag INTEGER,
    kassa INTEGER,
    nal REAL,
    beznal REAL,
    skidka REAL,
    num_chek_nal INTEGER,
    num_chek_beznal INTEGER,
    vozvrat_nal REAL,
    vozvrat_beznal REAL
    nal REAL,
    beznal REAL,
    skidka REAL,
    num_chek_nal INTEGER,
    num_chek_beznal INTEGER,
    vozvrat_nal REAL,
    vozvrat_beznal REAL
    );
"""

# функция для удаления и создания таблицы stage
def sqlite_try():
    try:
        sqlite_connection = sqlite3.connect('stage.db')
        cursor = sqlite_connection.cursor()
        print("База данных создана и успешно подключена к SQLite")

        cursor.execute(sql_drop_table)
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


sqlite_try()   # запуск запросов для stage

# функция для заполнения таблицы stage данными из рабочей базы
def update_stage(param):
    try:
        sqlite_connection = sqlite3.connect('stage.db')
        cursor = sqlite_connection.cursor()
        print("База данных успешно подключена к SQLite")

        sqlite_insert_query = """INSERT INTO stage1
                            (date, mag, kassa, nal, beznal, skidka, num_chek_nal, num_chek_beznal, vozvrat_nal, vozvrat_beznal)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"""
                            (date, mag, kassa, nal, beznal, skidka, num_chek_nal, num_chek_beznal, vozvrat_nal, vozvrat_beznal)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"""
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