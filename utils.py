import pytz
from math import ceil
from config import LENGTH
from datetime import datetime, timedelta, timezone


# Функция разбивает интервал от _start до _stop на quantity интервалов
# Возвращает список из quantity элементов.
# Один элемент - это объект с ключами "start" и "stop":
# "start" - это айдишник с которого мы начинаем собирать данные
# "stop" - это айдишник, на котором заканчиваем собирать данные
def get_ranges(quantity, _start, _stop):
    x = []

    for i in range(quantity):
        x.append({"start": _start, "stop": round(_start + _stop / quantity - 1)})
        _start += round(_stop / quantity)

    return x


# Обрезает из строки сайта гет-запросы и хеши
def get_site(site):
    if not site:
        return

    index = site.find('?')

    if index != -1:
        site = site[0: index]

    index_hash = site.find('#')

    if index_hash != -1:
        site = site[0: index_hash]

    return site


# Получаем массив с айдишниками.
# Возвращаем массив строк. Каждая строка - 500 айдишников через запятую
def get_row_ids(arr):
    x = []
    step_quantity = ceil(len(arr) / LENGTH)

    for i in range(step_quantity):
        x.append(','.join(arr[LENGTH * i: LENGTH * (i + 1)]))

    return x


# Возвращает массив с массивами, каждый из которых длинной quantity
def get_arr_ids(arr, quantity):
    x = []
    step_quantity = ceil(len(arr) / quantity)

    for i in range(step_quantity):
        x.append(arr[quantity * i: quantity * (i + 1)])

    return x


# Возвращает unix-дату.
# Принимает дату в виде строки
def get_unix_from_string_date(date_str):
    day, month, year = date_str.split('-')
    date = datetime(int(year), int(month), int(day), tzinfo=timezone.utc)

    return int(date.timestamp())


# Возвращает дату в формате sql
# Принимает unix-дату
def get_sql_date_from_unix(unix):
    date = datetime.fromtimestamp(unix, tz=timezone.utc)

    return date.strftime('%Y-%m-%d')


# Возвращает дату в unix формате увеличенную на интервал
# start - стартовая дата в формате unix
# interval - интервал в днях
def add_interval_days(start, interval):
    date = datetime.fromtimestamp(start, tz=timezone.utc)
    stop = date + timedelta(days=interval)

    return int(stop.timestamp())


# Генератор, который возвращает нам по одному элементу массива за раз
# После оканчания массива, возвращается к его началу
def items_generator(arr):
    active = 0

    while True:
        yield arr[active]

        active = 0 if active == len(arr) - 1 else active + 1