import pymysql
from config import *


def get_weeks_from_db():
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
                sql = 'SELECT start_date, stop_date FROM `weeks`'
                cursor.execute(sql)

                weeks = list(map(lambda x: [x['start_date'], x['stop_date']], cursor.fetchall()))

                return weeks

        finally:
            connection.close()
    except Exception as ex:
        print('Some wrong in get_weeks_from_db')
        print(ex)
