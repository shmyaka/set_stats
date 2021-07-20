from weeks_from_db import get_weeks_from_db
import asyncio
import aiohttp
from time import time
from math import ceil
from get_not_full_date import get_not_full_date
from execute_stats import get_execute_stats
from utils import get_sql_date_from_unix

interval_days = 7
delay = 0.34
start_script_time = time()
moment = time()


# Заносим данные о пяти топ-городов в таблицу БД
# На вход принимаем массив данных (город-количество посетителей)
# Возвращаем айдишник новой записи в таблице cities
def set_cities(connect, cities):
    try:
        if not cities:
            return None

        arr = []
        _count = 0

        for city in cities:
            _count += city.get('count')
            arr.append((city.get('value'), city.get('count')))

        arr = sorted(arr, key=lambda x: x[1], reverse=True)

        _top_1_city = arr[0][0] if len(arr) > 0 else None
        _top_1_percent = round(arr[0][1] / _count * 100, 2) if len(arr) > 0 else None
        _top_2_city = arr[1][0] if len(arr) > 1 else None
        _top_2_percent = round(arr[1][1] / _count * 100, 2) if len(arr) > 1 else None
        _top_3_city = arr[2][0] if len(arr) > 2 else None
        _top_3_percent = round(arr[2][1] / _count * 100, 2) if len(arr) > 2 else None
        _top_4_city = arr[3][0] if len(arr) > 3 else None
        _top_4_percent = round(arr[3][1] / _count * 100, 2) if len(arr) > 3 else None
        _top_5_city = arr[4][0] if len(arr) > 4 else None
        _top_5_percent = round(arr[4][1] / _count * 100, 2) if len(arr) > 4 else None

        with connect.cursor() as cursor:

            sql = 'INSERT INTO `cities` (top_1_city, top_1_percent, top_2_city, top_2_percent, top_3_city, ' \
                  'top_3_percent, top_4_city, top_4_percent, top_5_city, top_5_percent) VALUES (%s, %s, %s, %s, ' \
                  '%s, %s, %s, %s, %s, %s)'

            cursor.execute(sql, (_top_1_city, _top_1_percent, _top_2_city, _top_2_percent, _top_3_city, _top_3_percent,
                                 _top_4_city, _top_4_percent, _top_5_city, _top_5_percent))

            connect.commit()

            sql = 'SELECT LAST_INSERT_ID() as id'
            cursor.execute(sql)
            r = cursor.fetchone()

            return r['id']

    except Exception as ex:
        print('Error in set_cities')
        print(ex)


# Заносим данные о возрастах посетителей в таблицу БД
# На вход принимаем массив данных (возраст-количество посетителей)
# Возвращаем айдишник новой записи в таблице age
def set_ages(connect, ages):
    try:
        if not ages:
            return None

        age_dict = {
            '12-18': None,
            '18-21': None,
            '21-24': None,
            '24-27': None,
            '27-30': None,
            '30-35': None,
            '35-45': None,
            '45-100': None
        }

        for age in ages:
            age_dict[age['value']] = age['count']

        with connect.cursor() as cursor:
            sql = 'INSERT INTO `age` (`12-18`, `18-21`, `21-24`, `24-27`, `27-30`, `30-35`, `35-45`, `45-100`) ' \
                  'VALUES (%s, %s, %s, %s, %s, %s, %s, %s)'

            cursor.execute(sql, (age_dict.get('12-18'), age_dict.get('18-21'), age_dict.get('21-24'),
                                 age_dict.get('24-27'), age_dict.get('27-30'), age_dict.get('30-35'),
                                 age_dict.get('35-45'), age_dict.get('45-100')))

            connect.commit()

            sql = 'SELECT LAST_INSERT_ID() as id'
            cursor.execute(sql)
            r = cursor.fetchone()

            return r['id']

    except Exception as ex:
        print('Error in set_ages')
        print(ex)


# Заносим данные о поле и возрастах посетителей в таблицу БД
# На вход принимаем массив данных (возраст;пол-количество посетителей)
# Возвращаем айдишник новой записи в таблице sex-age
def set_sex_age(connect, sex_age):
    try:
        if not sex_age:
            return None

        sex_age_dict = {
            'f;12-18': None,
            'f;18-21': None,
            'f;21-24': None,
            'f;24-27': None,
            'f;27-30': None,
            'f;30-35': None,
            'f;35-45': None,
            'f;45-100': None,
            'm;12-18': None,
            'm;18-21': None,
            'm;21-24': None,
            'm;24-27': None,
            'm;27-30': None,
            'm;30-35': None,
            'm;35-45': None,
            'm;45-100': None
        }

        for age in sex_age:
            sex_age_dict[age['value']] = age['count']

        with connect.cursor() as cursor:
            sql = 'INSERT INTO `sex_age` (`f;12-18`, `f;18-21`, `f;21-24`, `f;24-27`, `f;27-30`, `f;30-35`, ' \
                  '`f;35-45`, `f;45-100`, `m;12-18`, `m;18-21`, `m;21-24`, `m;24-27`, `m;27-30`, `m;30-35`, ' \
                  '`m;35-45`, `m;45-100`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'

            cursor.execute(sql, (sex_age_dict.get('f;12-18'), sex_age_dict.get('f;18-21'), sex_age_dict.get('f;21-24'),
                                 sex_age_dict.get('f;24-27'), sex_age_dict.get('f;27-30'), sex_age_dict.get('f;30-35'),
                                 sex_age_dict.get('f;35-45'), sex_age_dict.get('f;45-100'), sex_age_dict.get('m;12-18'),
                                 sex_age_dict.get('m;18-21'), sex_age_dict.get('m;21-24'), sex_age_dict.get('m;24-27'),
                                 sex_age_dict.get('m;27-30'), sex_age_dict.get('m;30-35'), sex_age_dict.get('m;35-45'),
                                 sex_age_dict.get('m;45-100')))

            connect.commit()

            sql = 'SELECT LAST_INSERT_ID() as id'
            cursor.execute(sql)
            r = cursor.fetchone()

            return r['id']

    except Exception as ex:
        print('Error in set_sex_age')
        print(ex)


# Расчитываем и возвращаем процент посетилелей из РФ
def get_countries_percent(countries):
    try:
        _ru = 0
        _count = 0

        for country in countries:
            _count += country.get('count')

            if country.get('value') == 1:
                _ru += country.get('count')

        return 0 if _count == 0 else round(_ru / _count * 100, 2)
    except Exception as ex:
        print('Error in get_countries_percent')
        print(ex)


# Получаем кортеж данных, для занесения их в БД
def get_data_stats_from_db(connect, group_id, interval, data):
    try:
        _activity = data.get('activity')
        _date_from = get_sql_date_from_unix(data.get('period_from'))
        _views = data.get('visitors').get('views')
        _visitors = data.get('visitors').get('visitors')
        _reach_obj = data.get('reach')
        _reach = _reach_obj.get('reach')
        _reach_subscribers = _reach_obj.get('reach_subscribers')
        _mobile_reach = _reach_obj.get('mobile_reach')
        _sex_f = None
        _sex_m = None
        _country_ru = get_countries_percent(_reach_obj.get('countries'))
        _likes = _activity.get('likes') if _activity else None
        _comments = _activity.get('comments') if _activity else None
        _copies = _activity.get('copies') if _activity else None
        _hidden = _activity.get('hidden') if _activity else None
        _subscribed = _activity.get('subscribed') if _activity else None
        _unsubscribed = _activity.get('unsubscribed') if _activity else None

        if not _activity and not _reach and not _mobile_reach and not _reach_subscribers \
                and not _views and not _visitors:
            return

        # Заносим топ городов в таблицу и возвращаем айдишник этой записи
        _city_id = set_cities(connect, _reach_obj.get('cities'))

        # Заносим посетителей по возрасту в таблицу и возвращаем айдишник этой записи
        _age_id = set_ages(connect, _reach_obj.get('age'))

        # Заносим посетителей по полу и возрасту в таблицу и возвращаем айдишник этой записи
        _sex_age_id = set_sex_age(connect, _reach_obj.get('sex_age'))

        if _reach_obj.get('sex'):
            for sex in _reach_obj.get('sex'):
                if sex.get('value') == 'f':
                    _sex_f = sex.get('count')
                elif sex.get('value') == 'm':
                    _sex_m = sex.get('count')

        return (group_id, _date_from, interval, _views, _visitors, _reach, _reach_subscribers, _mobile_reach, _sex_f,
                _sex_m, _age_id, _sex_age_id, _city_id, _country_ru, _likes,
                _comments, _copies, _hidden, _subscribed, _unsubscribed)

    except Exception as ex:
        print('Error in get_data_stats_from_db')
        print(ex)


# Асинхронно заносим данные статистике для заданной группы, в интервале weeks (массив с датами начала статистики)
async def process_stats_data(connect, session, group_id, is_full_weeks=False):
    global moment

    if not is_full_weeks:
        weeks = get_weeks_from_db()
    else:
        weeks = get_not_full_date(connect, group_id)

    q = ceil(len(weeks) / 25)
    print(f'START NUMBER - {group_id}')

    for i in range(q):
        # Вычисляем задержку, перед запросом
        _sleep = 0 if time() > moment + delay else moment - time() + delay

        # Записываем запланированное время запроса данного экземпляра функции
        moment = time() + _sleep

        # Останавливаем выполнение функции, пока не прийдет её очередь вызова
        await asyncio.sleep(_sleep)

        week = weeks[25 * i: 25 * (i + 1)]
        stats = await get_execute_stats(session, week, group_id, interval_days)

        with connect.cursor() as cursor:
            if not stats or not list(filter(lambda x: bool(x), stats)):
                print('NOT STATS')
                break

            for item_data in stats:
                data = get_data_stats_from_db(connect, group_id, interval_days, item_data)

                if not data:
                    continue

                sql = 'INSERT INTO `statistics` (group_id, date_from, _interval, views, visitors, reach, ' \
                      'reach_subscribers, mobile_reach, sex_f, sex_m, age_id, sex_age_id, city_id, country_ru, ' \
                      'likes, comments, copies, hidden, subscribed, unsubscribed) VALUES (%s, %s, %s, %s, %s, %s, ' \
                      '%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);'

                try:
                    cursor.execute(sql, data)
                    connect.commit()
                except Exception as excep:
                    print(excep)
                    try:
                        if excep.args[0] == 1062:
                            print("DUPLICATE ERROR")
                            continue
                    except:
                        pass

    # Выводим время, потребовшееся на выполнение всех запросов в интервале
    print(f'{group_id} end - {time() - start_script_time}')


# Асинхронно запускаем функции записи статистики для всех групп из массива айдишников
async def set_stats(connect, ids_arr, is_full_weeks=False):
    tasks = []

    async with aiohttp.ClientSession() as session:
        for item_id in ids_arr:
            task = asyncio.create_task(process_stats_data(connect, session, item_id, is_full_weeks=is_full_weeks))
            tasks.append(task)

        await asyncio.gather(*tasks)

    # Ждём секунду перед завершением функции, чтобы успели завершиться все синхронные команды и не выскакивала ошибка
    await asyncio.sleep(1)
