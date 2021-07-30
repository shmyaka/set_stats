import pymysql
from config import *
import asyncio
from get_ids import get_all_ids_from_bd
from get_top_100_ids import get_top_100_ids
from get_not_full_stat_ids import get_not_full_stat_ids
from set_execute_stats import set_stats
import datetime


def run_collecting_statistics(connection, log_name, ids, is_full_weeks=False, k=False, start_date=False, stop_date=False):
    # Записываем в файл номер очереди и айдишники, которые мы будем проходить
    with open(f'logs/{log_name}---log.txt', 'a') as file:
        file.write(str(datetime.datetime.now()) + '\n')
        file.write(str(k) + '\n')
        file.write(str(ids) + '\n')

    for i_arr in ids:
        print('_____GET STARTED_____')
        print(i_arr)

        # Записываем в логфайл время начала обработки массива из 10 айдишников и сам массив
        with open(f'logs/{log_name}---log.txt', 'a') as file:
            file.write('    _ _ _ _ _ _ _ _ _ _ _    \n \n')
            file.write(str(datetime.datetime.now()) + ' - ' + str(i_arr) + '\n')

        # Запускаем функцию сбора статистики для этих 10 групп
        asyncio.run(set_stats(connection, i_arr, is_full_weeks, start_date=start_date, stop_date=stop_date))


def execute_collecting_statistics(is_full_weeks=False, start_count_100=False, stop_count_100=False, start_date=False, stop_date=False):
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
            log_name = datetime.datetime.now().strftime('%d-%m-%Y--%H-%M')

            if is_full_weeks:
                ids = get_not_full_stat_ids(10)
                run_collecting_statistics(connection, log_name, ids, is_full_weeks)

            else:
                for k in range(start_count_100, stop_count_100):
                    # Собираем айдишники очередных 100 популярных групп
                    ids = get_top_100_ids(10, k)

                    run_collecting_statistics(connection, log_name, ids, k=k, start_date=start_date, stop_date=start_date)
        finally:
            connection.close()
    except Exception as ex:
        error_log_name = datetime.datetime.now().strftime('%d-%m-%Y--%H')
        with open(f'logs/{error_log_name}---error.txt', 'a') as file:
            file.write(datetime.datetime.now().strftime('%d-%m-%Y--%H-%M'))
            file.write('\n \n')
            file.write(str(ex))
            file.write('\n \n')
            print('Some wrong in collecting statistics')
            print(ex)


