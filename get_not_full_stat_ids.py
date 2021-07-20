import pymysql
from config import *
from utils import get_arr_ids
from math import ceil

# full_weeks_quantity = 286


# Получаем айдишники групп, данные по которым не были полностью собраны
# quantity - количество айдишников в подмассиве (тогда возвращаем массив массивов по quantity айдишников)
# Если quantity не передано, возвращаем просто массив с айдишниками
def get_not_full_stat_ids(quantity=False):
    try:
        # Соединяемся с БД
        connection = pymysql.connect(
            host=host,
            port=3306,
            user=user,
            password=password,
            charset='utf8mb4',
            database='stat',
            cursorclass=pymysql.cursors.DictCursor
        )
        print('success')

        try:
            with connection.cursor() as cursor:
                cursor.execute('SELECT count(1) as c FROM stat.weeks;')

                full_weeks_quantity = int(cursor.fetchone().get('c'))

                sql = f'SELECT group_id, count(id) as c FROM `statistics` GROUP BY group_id ' \
                      f'HAVING c < {full_weeks_quantity}'

                cursor.execute(sql)

                ids_arr = list(map(lambda x: x['group_id'], cursor.fetchall()))

                if quantity:
                    return get_arr_ids(ids_arr, quantity)

                return ids_arr
        finally:
            connection.close()
    except Exception as ex:
        print('Some wrong in get_not_full_stat_ids')
        print(ex)

get_not_full_stat_ids()