from execute_collecting_statistics import execute_collecting_statistics
from vars.vars import is_full_weeks, stop_count, start_count

if is_full_weeks:
    # Запускаем сбор статистики для "неполных" групп
    while True:
        execute_collecting_statistics(True)
else:
    # Запускаем сбор статистики для популярных групп
    # start_count_100 - это стартовый порядковый номер сотни самых популярных групп.
    # То есть 0 - это первая сотя. 1 - это группы с 101 по 200, и так далее
    # stop_count_100 - это финишный порядковый сотни самых популярных групп.
    # То есть, если мы хотим собрать статистико по 10000 семых популярных групп,
    # то нам нужно ввдодить start_count_100 = 0, stop_count_100 = 101
    execute_collecting_statistics(start_count_100=start_count, stop_count_100=stop_count)
