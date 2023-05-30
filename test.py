import sqlite3

def sqlite_try():
    try:
        sqlite_connection = sqlite3.connect('stage.db')
        cursor = sqlite_connection.cursor()
        print("База данных создана и успешно подключена к SQLite")

        cursor.execute("""select * from stage1;""")
        print(*cursor.fetchall(), sep='\n')

        cursor.close()

    except sqlite3.Error as error:
        print("Ошибка при подключении к sqlite", error)
    finally:
        if (sqlite_connection):
            sqlite_connection.close()
            print("Соединение с SQLite закрыто")


sqlite_try()