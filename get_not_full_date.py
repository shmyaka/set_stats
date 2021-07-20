import pymysql
from config import *


# Возвращает массив дат, в формате unix, для которых в БД нет данных, для указанной группы
# group_id - айдишник группы, которую мы проверяем
def get_not_full_date(connection, group_id):
    try:
        with connection.cursor() as cursor:
            sql = f'select w.start_date, w.stop_date FROM (SELECT * FROM statistics ' \
                  f'where group_id ="{group_id}") as pop ' \
                  f'right join stat.weeks as w on w.str_date = pop.date_from where pop.group_id is NULL'

            cursor.execute(sql)

            return list(map(lambda x: [x['start_date'], x['stop_date']], cursor.fetchall()))
    except Exception as ex:
        print('Some wrong in get_not_full_date')
        print(ex)

