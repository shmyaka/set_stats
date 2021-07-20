import requests as rq
from config import tokens
from datetime import datetime, timedelta
from utils import items_generator


url = 'https://api.vk.com/method/stats.get'
params = {
    'v': '5.139',
}
get_current_token = items_generator(tokens)


def add_five_year(date_str):
    d = datetime.strptime(date_str, '%d-%m-%Y')
    new_d = datetime(d.year + 5, d.month, d.day)

    return datetime.strftime(new_d, '%d-%m-%Y')


date_from = '01-01-2016'
date_to = add_five_year(date_from)


async def get_stats(session, id, date_from, date_to=None, interval='week'):
    params['group_id'] = id
    params['timestamp_from'] = date_from
    params['interval'] = interval
    params['access_token'] = next(get_current_token)

    if date_to:
        params['timestamp_to'] = date_to

    async with session.get(url, params=params) as r:
        r_json = await r.json()
        return r_json.get('response')
