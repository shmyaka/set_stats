import pymysql
from config import *
from utils import get_row_ids, get_arr_ids


# Получаем все айдишники из таблицы active_ids
# Возвращаем массив строк. Каждая строка - 500 айдишников через запятую
def get_all_ids_from_bd(quantity=False):
    try:
        # Соединяемся с БД
        connection = pymysql.connect(
            host=host,
            port=3306,
            user=user,
            password=password,
            charset='utf8mb4',
            database=db_name,
            cursorclass=pymysql.cursors.DictCursor
        )
        print('success')

        try:
            with connection.cursor() as cursor:
                sql = 'SELECT id FROM `active_ids`'
                cursor.execute(sql)

                rows = cursor.fetchall()
                ids_arr = list(map(lambda x: str(x['id']), rows))

                if quantity:
                    return get_arr_ids(ids_arr, quantity)

                return get_row_ids(ids_arr)
        finally:
            connection.close()
    except Exception as ex:
        print('Some wrong')
        print(ex)
