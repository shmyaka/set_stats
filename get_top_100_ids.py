import pymysql
from config import *
from utils import get_row_ids, get_arr_ids


# Получаем айдишники самых популярных групп
# quantity - количество айдишников в подмассиве (тогда возвращаем массив массивов по quantity айдишников)
# Если quantity не передано, возвращаем просто массив с айдишниками
def get_top_100_ids(quantity=False, limit=0):
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
                sql = f'SELECT ai.id FROM `active_ids` as ai JOIN `groups` as g ON ai.id = g.id ' \
                      f'ORDER BY members_count DESC LIMIT {limit * 100}, 100'
                cursor.execute(sql)

                rows = cursor.fetchall()
                ids_arr = list(map(lambda x: str(x['id']), rows))

                if quantity:
                    return get_arr_ids(ids_arr, quantity)

                return ids_arr
        finally:
            connection.close()
    except Exception as ex:
        print('Some wrong')
        print(ex)


# print(get_top_100_ids(10, 0))